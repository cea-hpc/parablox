# author: Quentin Bouget <quentin.bouget@cea.fr>
#
# pylint: disable=protected-access

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
        self.waiting = Event()

    def get_job(self):
        """
        Wait for the consume event and return a job
        """
        self.waiting.set()
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

    def test_cancel_jobs(self):
        """
        Upon setting the cancel event, jobs are re-processed

        JobFactory --> WaitingBlock --> DummyProcessBlock (--> zombie)
        """
        # This factory only produces the "end job"
        job_factory = JobFactory(tuple())
        waiting_block = WaitingBlock(parent=job_factory)
        block = DummyProcessBlock(parent=waiting_block)
        ZombieBlock(parent=block).die.set()

        job_factory.start()

        # Manually add jobs to block's queue
        for job in range(10):
            block.jobs.put(job)

        # Wait for waiting_block to block
        self.assertTrue(waiting_block.waiting.wait(timeout=1))
        waiting_block.waiting.clear()
        # Cancel waiting_block
        waiting_block.events["cancel"].set()
        waiting_block.event.set()

        # Let waiting_block process the cancel event
        waiting_block.consume.set()

        # block should receive a requeue event and requeue its jobs
        block.jobs.join()

        for _ in range(11): # 10 + "end job"
            self.assertTrue(waiting_block.waiting.wait(timeout=1))
            waiting_block.waiting.clear()
            waiting_block.consume.set()

        # Jobs get reprocessed
        self.assertCountEqual(range(10), iter(block.jobs.get(timeout=1)
                                              for _ in range(10)))

        job_factory.join(timeout=1)
        self.assertFalse(job_factory.is_alive())

    def test_dynamic_requeue(self):
        """
        Upon setting the requeue event jobs are sent back to parents

        JobFactory --> WaitingBlock --> DummyProcessBlock (--> zombie)
        """
        job_factory = JobFactory(tuple())
        waiting_block = WaitingBlock(parent=job_factory)
        block = DummyProcessBlock(parent=waiting_block)
        ZombieBlock(parent=block).die.set()

        job_factory.start()

        # Manually add jobs to block's queue
        for job in range(10):
            block.jobs.put(job)

        # Wait for waiting_block to block
        self.assertTrue(waiting_block.waiting.wait(timeout=1))
        waiting_block.waiting.clear()
        # Requeue from waiting_block
        waiting_block.events["requeue"].set()
        waiting_block.event.set()

        # Let waiting_block process the requeue event
        waiting_block.consume.set()

        # block should receive a requeue event and requeue its jobs
        block.jobs.join()

        requeued_jobs = []
        for _ in range(10):
            requeued_jobs.append(job_factory.jobs.get(timeout=1))
            job_factory.jobs.task_done()

        self.assertCountEqual(range(10), requeued_jobs)

        # Wait until the event is entirely processed
        self.assertTrue(waiting_block.waiting.wait(timeout=1))
        waiting_block.waiting.clear()

        # Publish "end job" manually
        job_factory.jobs.put(None)

        waiting_block.consume.set()

        job_factory.join(timeout=1)
        self.assertFalse(job_factory.is_alive())
