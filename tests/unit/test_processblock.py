# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
Test the ProcessBlock abstract class
"""

from multiprocessing import Event
from multiprocessing.queues import Queue
from threading import Thread
from unittest import TestCase
from unittest.mock import Mock

from parablox import ProcessingError, ProcessBlock


class DummyProcessBlock(ProcessBlock):
    """
    Dummy ProcessBlock subclass, just passes on its jobs
    """

    def process_job(self, job):
        return job

class ZombieBlock(ProcessBlock):
    """
    Dummy ProcessBlock subclass

    Zombie are used to force blocks to store jobs in their queue
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop.set()

    def process_job(self, job):
        raise RuntimeError("This method sould not be called")

    def is_alive(self):
        return True


class FailingProcessBlock(ProcessBlock):
    """
    Fails every job once before processing them successfully
    """

    def __init__(self, failed_event=None, failure_msg="test", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fail = False
        self._failure_msg = failure_msg
        self._failed_event = failed_event

    def process_job(self, job):
        if not self._fail:
            self._fail = True
            raise ProcessingError(self._failure_msg)
        if self._failed_event is not None:
            self._failed_event.set()
        self._fail = False
        return job


class TestProcessBlock(TestCase):
    """
    Test ProcessBlock's attributes and methods
    """

    def test_public_attributes(self):
        """
        Check that public attributes exist
        """
        process_block = DummyProcessBlock()
        # children exists, is iterable and is empty
        self.assertEqual(list(process_block.children), [])
        # Parent defaults to None
        self.assertIsNone(process_block.parent)
        # jobs is an instance of a multiprocessing.queues.Queue
        self.assertIsInstance(process_block.jobs, Queue)

    def test_isabstract(self):
        """
        A ProcessBlock must define a `process_job` method
        """
        self.assertRaises(TypeError, ProcessBlock)

    def test_link_blocks(self):
        """
        Link two blocks to a third one
        """
        parent = DummyProcessBlock()
        children = [DummyProcessBlock(parent=parent),
                    DummyProcessBlock(parent=parent),]
        self.assertEqual(list(parent.children), children)
        for child in children:
            self.assertEqual(child.parent, parent)

    def test_run(self):
        """
        Run a process block

        parent --> child (--> zombie)
        """
        parent = ZombieBlock() # Zombie <= allow child to stop
        child = DummyProcessBlock(parent=parent)
        ZombieBlock(parent=child)

        child.process_job = Mock(side_effect=lambda job: job + 1)

        child.start()
        self.assertTrue(child.is_alive())

        # Simulate parent producing jobs
        for i in range(10):
            parent.jobs.put(i, timeout=1)
            self.assertEqual(child.jobs.get(timeout=1), child.process_job(i))
        parent.jobs.put(None, timeout=1)

        child.join(timeout=1)
        self.assertFalse(child.is_alive())

    def test_warning_on_job_failure(self):
        """
        Log JopProcessingException as warnings
        """
        parent = DummyProcessBlock()
        job_failed = Event()
        child = FailingProcessBlock(failed_event=job_failed, parent=parent)

        # Logging + multiprocessing cannot be easily tested -> use a thread
        child_thr = Thread(target=child.run)
        with self.assertLogs(level='WARNING') as context_manager:
            child_thr.start()
            parent.jobs.put(True)
            # Wait for the job to actually fail
            self.assertTrue(job_failed.wait(timeout=1))

        # Stop child_thr
        parent.jobs.put(None)

        # Meanwhile: only one message was logged and it says "test"
        self.assertEqual(context_manager.records.pop().getMessage().strip(),
                         "test")

        # Join child_thr
        child_thr.join(timeout=1)
        self.assertFalse(child_thr.is_alive())
