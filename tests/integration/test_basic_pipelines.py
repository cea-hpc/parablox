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


class ObjFactory(DummyProcessBlock):
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
        Return a generator of objects
        """
        return iter(copy(self.iterable))

    def get_obj(self):
        """
        Get an object from the factory

        Set the stop event once every object is scheduled
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

    It waits for an event before getting an object from its parent
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consume = Event()
        self.waiting = Event()

    def get_obj(self):
        """
        Wait for the consume event and return an object
        """
        self.waiting.set()
        self.consume.wait()
        self.consume.clear()
        return super().get_obj()


class TestPipelines(TestCase):
    """
    Test some basic pipelines configuration
    """

    def test_basic_pipeline(self):
        """
        ObjFactory --> DummyProcessBlock (--> zombie)
        """
        obj_factory = ObjFactory(range(10))
        block = DummyProcessBlock(parent=obj_factory)

        # To be able to get the block's objects
        ZombieBlock(parent=block)

        obj_factory.start()
        for obj in obj_factory:
            self.assertEqual(block.objs.get(timeout=1), obj)

        obj_factory.join(timeout=1)
        self.assertFalse(obj_factory.is_alive())

    def test_multiple_blocks(self):
        """
        One ObjFactory with 2 blocks

                     --> WaitingBlock (--> zombie)
        ObjFactory --|
                     --> WaitingBlock (--> zombie)
        """
        obj_factory = ObjFactory(range(10))
        blocks = [WaitingBlock(parent=obj_factory),
                  WaitingBlock(parent=obj_factory),]

        # To be able to get the blocks' objects
        for block in blocks:
            ZombieBlock(parent=block)

        obj_factory.start()
        # Internal check, to ensure the next loop's condition is correct
        self.assertFalse(len(obj_factory) % len(blocks))

        blocks_cycle = cycle(blocks)
        for obj in obj_factory:
            block = next(blocks_cycle)
            block.consume.set()
            self.assertEqual(block.objs.get(timeout=1), obj)

        # Consume the end event
        for block in blocks:
            block.consume.set()
            self.assertIsNone(block.objs.get(timeout=1))

        obj_factory.join(timeout=1)
        self.assertFalse(obj_factory.is_alive())

    def test_cancel_objs(self):
        """
        Upon setting the cancel event, objs are re-processed

        ObjFactory --> WaitingBlock --> DummyProcessBlock (--> zombie)
        """
        # This factory only produces the "end object"
        obj_factory = ObjFactory(tuple())
        waiting_block = WaitingBlock(parent=obj_factory)
        block = DummyProcessBlock(parent=waiting_block)
        ZombieBlock(parent=block).die.set()

        obj_factory.start()

        # Manually add objects to block's queue
        for obj in range(10):
            block.objs.put(obj)

        # Wait for waiting_block to block
        self.assertTrue(waiting_block.waiting.wait(timeout=1))
        waiting_block.waiting.clear()
        # Cancel waiting_block
        waiting_block.events["cancel"].set()
        waiting_block.event.set()

        # Let waiting_block process the cancel event
        waiting_block.consume.set()

        # block should receive a requeue event and requeue its objs
        block.objs.join()

        for _ in range(11): # 10 + "end obj"
            self.assertTrue(waiting_block.waiting.wait(timeout=1))
            waiting_block.waiting.clear()
            waiting_block.consume.set()

        # objects get reprocessed
        self.assertCountEqual(range(10), iter(block.objs.get(timeout=1)
                                              for _ in range(10)))

        obj_factory.join(timeout=1)
        self.assertFalse(obj_factory.is_alive())

    def test_dynamic_requeue(self):
        """
        Upon setting the requeue event objects are sent back to parents

        ObjFactory --> WaitingBlock --> DummyProcessBlock (--> zombie)
        """
        obj_factory = ObjFactory(tuple())
        waiting_block = WaitingBlock(parent=obj_factory)
        block = DummyProcessBlock(parent=waiting_block)
        ZombieBlock(parent=block).die.set()

        obj_factory.start()

        # Manually add objects to block's queue
        for obj in range(10):
            block.objs.put(obj)

        # Wait for waiting_block to block
        self.assertTrue(waiting_block.waiting.wait(timeout=1))
        waiting_block.waiting.clear()
        # Requeue from waiting_block
        waiting_block.events["requeue"].set()
        waiting_block.event.set()

        # Let waiting_block process the requeue event
        waiting_block.consume.set()

        # block should receive a requeue event and requeue its objects
        block.objs.join()

        requeued_objs = []
        for _ in range(10):
            requeued_objs.append(obj_factory.objs.get(timeout=1))
            obj_factory.objs.task_done()

        self.assertCountEqual(range(10), requeued_objs)

        # Wait until the event is entirely processed
        self.assertTrue(waiting_block.waiting.wait(timeout=1))
        waiting_block.waiting.clear()

        # Publish "end object" manually
        obj_factory.objs.put(None)

        waiting_block.consume.set()

        obj_factory.join(timeout=1)
        self.assertFalse(obj_factory.is_alive())
