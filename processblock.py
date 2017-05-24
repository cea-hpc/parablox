# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
This modules provides an interface to easily pipe Process objects

Classes implemented in this module:

# ProcessBlock
Abstract class representing a process

# EventInterrupt
Exception to be raised when an event interrupts a process

# ProcessingError
Exception to be raised on error in ProcessBlock.proc
"""

from abc import ABC, abstractmethod
from collections import deque, namedtuple, OrderedDict
from copy import copy
from itertools import chain
from logging import getLogger
from multiprocessing import Event, Process, JoinableQueue
from queue import Empty, Full


class EventInterrupt(Exception):
    """
    To be raised when an event interrupts something
    """
    pass


class ProcessingError(RuntimeError):
    """
    To be raised when ProcessBlock's process_obj() method fails
    """
    pass


class BlockFamily(namedtuple('BlockFamily', "parent, siblings, children")):
    """
    A namedtuple to easily manage a block's lineage
    """

    __slots__ = ()

    def link(self, processblock):
        """
        Insert a processblock into a family
        """
        if self.parent is not None:
            # Link to siblings
            for sibling in self.siblings:
                sibling.family.siblings.append(processblock)
            # Link to the parent block
            self.parent.family.children.append(processblock)

    def __iter__(self):
        """
        Returns an iterator over parent, siblings and children

        If parent is None, the iterator returned will not yield it
        """
        if self.parent is not None:
            return chain((self.parent,), self.siblings, self.children)

        return chain(self.siblings, self.children)

    def alive_children(self):
        """
        Returns an iterator over children that are alive
        """
        return filter(lambda child: child.is_alive(), self.children)

    def is_stopped(self):
        """
        Is everyone in the family stopped?
        """
        return all(block.events["stop"].is_set() for block in self)


class ProcessBlock(Process, ABC):
    """
    The abstract class for a block/process in an execution pipeline
    """
    # Arbitrary timeout for blocking queue operations
    _poll_interval = 1

    def __init__(self, *args, parent=None, queue_size=0, **kwargs):
        super().__init__(*args, **kwargs)

        # Events (in the order they should be checked)
        self.events = OrderedDict([
            ("cancel", Event()),
            ("requeue", Event()),
            ("stop", Event()),
            ])

        # Corresponding event handlers
        self.event_handlers = {
            "cancel": self.cancel_handler,
            "requeue": self.requeue_handler,
            "stop": self.stop_handler,
            }

        # Master event, to be set after any other event
        self.event = Event()

        # The family of the processblock
        siblings = copy(parent.family.children) if parent is not None else []
        self.family = BlockFamily(parent, siblings, [])
        # Link family with self
        self.family.link(self)

        # The object queue
        self.objs = JoinableQueue(queue_size)
        # List of objects that were canceled and need re-processing
        self._canceled_objs = deque()

        # Logging facility
        self.logger = getLogger(self.name)

        # Object currently processed
        self._obj = None

    def start(self):
        super().__init__(name=self.name)
        super().start()

    @abstractmethod
    def process_obj(self, obj):
        """
        The actual work a block wants to perform on a object
        """
        raise NotImplementedError()

    def stop_handler(self):
        """
        Send the "end object" (None) to every child
        """
        self.logger.debug("sending the 'end object' to child processes...")
        for _ in self.family.alive_children():
            self.objs.put(None)

    def cancel_handler(self):
        """
        Cancel children's objects and re-queue them in self._canceled_objs
        """
        self.logger.debug("ask children to requeue their objects")
        for child in self.family.alive_children():
            child.events["requeue"].set()
            child.event.set()

        self.logger.debug("fetching canceled objects...")
        while (self.objs.qsize() != 0 or
               any(child.events["requeue"].is_set()
                   for child in self.family.alive_children())):
            try:
                obj = self.objs.get_nowait()
                self.objs.task_done()
            except Empty:
                continue
            if obj is not None:
                self._canceled_objs.append(obj)

        # To be able to stop without the parent block sending an 'end object'
        if self.events["stop"].is_set():
            self._canceled_objs.append(None)
            self.events["stop"].clear()

        # Clear the event
        self.events["cancel"].clear()

    def requeue_handler(self):
        """
        Requeue every object managed by the block or one of its children
        """
        for child in self.family.alive_children():
            child.events["requeue"].set()
            child.event.set()

        self.logger.debug("requeueing objects...")
        if self._obj is not None:
            self.family.parent.objs.put(self._obj)
            self._obj = None

        while (self.objs.qsize() != 0 or
               any(child.events["requeue"].is_set()
                   for child in self.family.alive_children())):
            try:
                obj = self.objs.get_nowait()
                self.objs.task_done()
            except Empty:
                # Do not waste that time
                if self._canceled_objs:
                    obj = self._canceled_objs.popleft()
                else:
                    continue
            if obj is not None:
                self.family.parent.objs.put(obj)

        for obj in filter(lambda x: x is not None, self._canceled_objs):
            self.family.parent.objs.put(obj)

        self.logger.debug("wait for parent to fetch all the objects...")
        self.family.parent.objs.join()

        # Processblock was potentially stopped
        self.events["stop"].clear()

        # Clear the event
        self.events["requeue"].clear()

    def process_events(self, ignore=()):
        """
        Process events

        The order in which events are processed is important
        Returns:
            True --- if an Event was processed
            False --- otherwise
        """
        self.logger.debug("process events...")
        if not self.event.is_set():
            return False
        self.event.clear()

        event_processed = False
        for event_name in self.events:
            if event_name in ignore:
                continue
            if self.events[event_name].is_set():
                self.logger.debug("processing '%s' event", event_name)
                self.event_handlers[event_name]()
                event_processed = True

        return event_processed

    def get_obj(self):
        """
        Get an object from the parent block
        """
        self.logger.debug("get an object to process...")
        try:
            return self._canceled_objs.popleft()
        except IndexError:
            obj = self.family.parent.objs.get(timeout=self._poll_interval)
            self.family.parent.objs.task_done()
            return obj

    def publish_obj(self, obj):
        """
        Publish `obj` to child blocks
        """
        if obj is None:
            # The block is batching objs or filtering them
            self.logger.debug("no obj to pass on")
            return

        self.logger.debug("publish '%s'", obj)
        if self.family.children:
            self.objs.put(obj, timeout=self._poll_interval)

    def cleanup(self):
        """
        Tell parent and siblings we stop and exit cleanly
        """
        if self.family.parent is not None:
            self.family.parent.event.set()
        for sibling in self.family.siblings:
            sibling.event.set()
        self.logger.debug("waiting for child processes...")
        for child in self.family.children:
            child.join()

    def run(self):
        """
        Launch child blocks and process objects
        """
        # Launch child blocks
        # Children are started here in order to build a gracefull process tree
        self.logger.debug("start %d child(ren)", len(self.family.children))
        for child in self.family.children:
            child.start()

        while not self.events["stop"].is_set():
            # Processing loop
            while not self.events["stop"].is_set():
                # Process exterior events
                if self.process_events():
                    continue

                # Find an object to process
                if self._obj is None:
                    try:
                        self._obj = self.get_obj()
                    except Empty:
                        continue

                    if self._obj is None:
                        self.logger.debug("received the 'end object'")
                        self.events["stop"].set()
                        self.event.set()
                        continue

                obj = self._obj

                # Process the object
                self.logger.debug("process '%s'", obj)
                try:
                    obj = self.process_obj(obj)
                except ProcessingError as exc:
                    self.logger.warning(exc)
                    continue
                except EventInterrupt:
                    # An event ocrrured, process it
                    continue

                # Publish the processed object, check for events periodically
                while not self.event.is_set():
                    try:
                        self.publish_obj(obj)
                    except Full:
                        continue
                    # Object was published, or did not to be
                    self._obj = None
                    break

            # Process the stop event (which is ignored in the loop underneath)
            self.process_events()

            # Wait for the entire family to stop, unless `stop` gets cleared
            while (self.events["stop"].is_set() and
                   not self.family.is_stopped()):
                self.event.wait()
                self.process_events(ignore=("stop",))

        # Process is exiting, there is no turning back
        # Every sibling/child process will shortly do so too (or already have)
        self.cleanup()
        self.logger.debug("terminating")
