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
    To be raised when ProcessBlock's process_job() method fails
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

        # The job queue
        self.jobs = JoinableQueue(queue_size)
        # List of jobs that were canceled and need re-processing
        self._canceled_jobs = deque()

        # Logging facility
        self.logger = getLogger(self.name)

        # Job currently processed
        self._job = None

    def start(self):
        super().__init__(name=self.name)
        super().start()

    @abstractmethod
    def process_job(self, job):
        """
        The actual work a block wants to perform on a job
        """
        raise NotImplementedError()

    def stop_handler(self):
        """
        Send the "end job" (None) to every child
        """
        self.logger.debug("sending the 'end job' to child processes...")
        for _ in self.family.alive_children():
            self.jobs.put(None)

    def cancel_handler(self):
        """
        Cancel children's jobs and re-queue them in self._canceled_jobs
        """
        self.logger.debug("ask children to requeue their jobs")
        for child in self.family.alive_children():
            child.events["requeue"].set()
            child.event.set()

        self.logger.debug("fetching canceled jobs...")
        while (self.jobs.qsize() != 0 or
               any(child.events["requeue"].is_set()
                   for child in self.family.alive_children())):
            try:
                job = self.jobs.get_nowait()
                self.jobs.task_done()
            except Empty:
                continue
            if job is not None:
                self._canceled_jobs.append(job)

        # To be able to stop without the parent block sending an 'end job'
        if self.events["stop"].is_set():
            self._canceled_jobs.append(None)
            self.events["stop"].clear()

        # Clear the event
        self.events["cancel"].clear()

    def requeue_handler(self):
        """
        Requeue every job managed by the block or one of its children
        """
        for child in self.family.alive_children():
            child.events["requeue"].set()
            child.event.set()

        self.logger.debug("requeueing jobs...")
        if self._job is not None:
            self.family.parent.jobs.put(self._job)
            self._job = None

        while (self.jobs.qsize() != 0 or
               any(child.events["requeue"].is_set()
                   for child in self.family.alive_children())):
            try:
                job = self.jobs.get_nowait()
                self.jobs.task_done()
            except Empty:
                # Do not waste that time
                if self._canceled_jobs:
                    job = self._canceled_jobs.popleft()
                else:
                    continue
            if job is not None:
                self.family.parent.jobs.put(job)

        for job in filter(lambda x: x is not None, self._canceled_jobs):
            self.family.parent.jobs.put(job)

        self.logger.debug("wait for parent to fetch all the jobs...")
        self.family.parent.jobs.join()

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

    def get_job(self):
        """
        Get a job from the parent block
        """
        self.logger.debug("get a job to process...")
        try:
            return self._canceled_jobs.popleft()
        except IndexError:
            job = self.family.parent.jobs.get(timeout=self._poll_interval)
            self.family.parent.jobs.task_done()
            return job

    def publish_job(self, job):
        """
        Publish `job` to child blocks
        """
        if job is None:
            # The block is batching jobs or filtering them
            self.logger.debug("no job to pass on")
            return

        self.logger.debug("publish '%s'", job)
        if self.family.children:
            self.jobs.put(job, timeout=self._poll_interval)

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
        Launch child blocks and process jobs
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

                # Find a job
                if self._job is None:
                    try:
                        self._job = self.get_job()
                    except Empty:
                        continue

                    if self._job is None:
                        self.logger.debug("received the 'end job'")
                        self.events["stop"].set()
                        self.event.set()
                        continue

                job = self._job

                # Process the job
                self.logger.debug("process a job...")
                try:
                    job = self.process_job(job)
                except ProcessingError as exc:
                    self.logger.warning(exc)
                    continue
                except EventInterrupt:
                    # An event ocrrured, process it
                    continue

                # Publish the processed job, check for events periodically
                while not self.event.is_set():
                    try:
                        self.publish_job(job)
                    except Full:
                        continue
                    # Job was published or did not to be
                    self._job = None
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
