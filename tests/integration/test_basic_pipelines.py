# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
Test a basic parablox pipeline
"""

from copy import copy
from itertools import cycle
from multiprocessing import Event
from unittest import TestCase

from parablox.tests.unit.test_processblock import (DummyProcessBlock,
                                                   ZombieBlock)


class JobFactory(DummyProcessBlock):
    """
    The first block of a pipeline, for testing purposes
    """

    def __init__(self, iterable, *args, **kwargs):
        super().__init__(parent=None, *args, **kwargs)
        self.iterable = iterable
        self.__len = len(list(copy(iterable)))
        self.__generator = None

    def __iter__(self):
        """
        Return a generator of jobs
        """
        return iter(copy(self.iterable))

    def get_job(self):
        """
        Get a job from the factory

        Set the stop event once every job is scheduled
        """
        if self.__generator is None:
            self.__generator = iter(self)
        try:
            return next(self.__generator)
        except StopIteration:
            self.__generator = None
            return None

    def __len__(self):
        return self.__len


class WaitingBlock(DummyProcessBlock):
    """
    Special ProcessBlock for test purposes

    It waits for an event before getting a job from its parent
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consume = Event()

    def get_job(self):
        """
        Wait for the consume event and return a job
        """
        self.consume.wait()
        self.consume.clear()
        return super().get_job()


class TestPipelines(TestCase):
    """
    Test some basic pipelines configuration
    """

    def test_basic_pipeline(self):
        """
        JobFactory --> DummyProcessBlock (--> zombie)
        """
        job_factory = JobFactory(range(10))
        block = DummyProcessBlock(parent=job_factory)

        # To be able to get the block's jobs
        ZombieBlock(parent=block)

        job_factory.start()
        for job in job_factory:
            self.assertEqual(block.jobs.get(timeout=1), job)

        job_factory.join(timeout=1)
        self.assertFalse(job_factory.is_alive())

    def test_multiple_blocks(self):
        """
        One JobFactory with 2 blocks

                     --> WaitingBlock (--> zombie)
        JobFactory --|
                     --> WaitingBlock (--> zombie)
        """
        job_factory = JobFactory(range(10))
        blocks = [WaitingBlock(parent=job_factory),
                  WaitingBlock(parent=job_factory),]

        # To be able to get the blocks' jobs
        for block in blocks:
            ZombieBlock(parent=block)

        job_factory.start()
        # Internal check, to ensure the next loop's condition is correct
        self.assertFalse(len(job_factory) % len(blocks))

        blocks_cycle = cycle(blocks)
        for job in job_factory:
            block = next(blocks_cycle)
            block.consume.set()
            self.assertEqual(block.jobs.get(timeout=1), job)

        # Consume the end event
        for block in blocks:
            block.consume.set()
            self.assertIsNone(block.jobs.get(timeout=1))

        job_factory.join(timeout=1)
        self.assertFalse(job_factory.is_alive())
