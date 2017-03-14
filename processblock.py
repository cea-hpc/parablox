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
from logging import getLogger
from multiprocessing import Event, Process, Queue


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


class ProcessBlock(Process, ABC):
    """
    The abstract class for a block/process in an execution pipeline
    """

    def __init__(self, *args, parent=None, queue_size=0, **kwargs):
        super().__init__(*args, **kwargs)

        # Events
        self.stop = Event()

        # A list of child blocks
        self.children = []

        # Link to the parent block, if any
        self.parent = parent
        if parent is not None:
            self.parent.children.append(self)

        # The job queue
        self.jobs = Queue(queue_size)

        # Logging facility
        self.logger = getLogger(self.name)

    def start(self):
        super().__init__(name=self.name)
        super().start()

    @abstractmethod
    def process_job(self, job):
        """
        The actual work a block wants to perform on a job
        """
        raise NotImplementedError()

    def process_events(self):
        """
        Process any event that has occured
        """
        self.logger.debug("process events...")

    def get_job(self):
        """
        Get a job from the parent block
        """
        self.logger.debug("get a job to process...")
        return self.parent.jobs.get()

    def run(self):
        """
        Launch child blocks and process jobs
        """
        # Launch child blocks
        # Children are started here in order to build a gracefull process tree
        self.logger.debug("start %d child processes", len(self.children))
        for child in self.children:
            child.start()

        # Infinite loop of processing jobs
        job = None
        while not self.stop.is_set():
            try:
                # Process exterior events
                self.process_events()

                if job is None:
                    job = self.get_job()
                    if job is None:
                        self.logger.debug("received the 'end job'")
                        self.stop.set()
                        continue

                # Process the job
                self.logger.debug("process a job...")
                try:
                    job = self.process_job(job)
                except ProcessingError as exc:
                    self.logger.warning(exc)
                    continue

                # The block is batching jobs or filtering them
                if job is None:
                    self.logger.debug("no job to pass on")
                    continue

                # job is set to None when the next line completes
                job = self.jobs.put(job) if self.children else None
            except EventInterrupt:
                continue

        for child in self.children:
            self.logger.debug("sending the 'end' job to a child process...")
            self.jobs.put(None)
        for child in self.children:
            self.logger.debug("waiting for a child process...")
            child.join()
        self.logger.debug("terminating")
