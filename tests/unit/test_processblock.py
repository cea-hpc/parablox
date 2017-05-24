# author: Quentin Bouget <quentin.bouget@cea.fr>
#
# pylint: disable=protected-access

"""
Test the ProcessBlock abstract class
"""

from collections import OrderedDict
from itertools import chain
from multiprocessing import Event
from multiprocessing.queues import JoinableQueue, Empty, Full
from multiprocessing.synchronize import Event as EventClass
from threading import Thread
from unittest import TestCase
from unittest.mock import Mock

from parablox import ProcessingError, ProcessBlock
from parablox.processblock import BlockFamily


class DummyProcessBlock(ProcessBlock):
    """
    Dummy ProcessBlock subclass, just passes on its objects
    """

    def process_obj(self, obj):
        return obj

class ZombieBlock(ProcessBlock):
    """
    Dummy ProcessBlock subclass

    Zombie are used to force blocks to store objects in their queue
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events["stop"].set()
        # Use an event to interact from another process's context
        self.die = Event()

    def process_obj(self, obj):
        raise RuntimeError("This method sould not be called")

    def is_alive(self):
        return not self.die.is_set()


class FailingProcessBlock(ProcessBlock):
    """
    Fails every object once before processing them successfully
    """

    def __init__(self, failure_msg="test", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._failure_msg = failure_msg

    def process_obj(self, obj):
        self._obj = None # Discard the job
        raise ProcessingError(self._failure_msg)


class TestBlockFamily(TestCase):
    """
    Test BlockFamily's methods
    """

    def test_link_noparent(self):
        """
        Link an orphaned block
        """
        self.assertEqual(DummyProcessBlock().family, BlockFamily(None, [], []))

    def test_link_children(self):
        """
        Link children to a parent

        The BlockFamily class is too tightly tied to ProcessBlock to test
        the link method explicitely, this test relies on how a ProcessBlock
        uses the BlockFamily's link method
        """
        parent = DummyProcessBlock()
        children = [DummyProcessBlock(parent=parent),
                    DummyProcessBlock(parent=parent),
                    DummyProcessBlock(parent=parent),]

        # parent's family include the children
        self.assertEqual(parent.family, BlockFamily(None, [], children))

        # children's parent and siblings are correctly set
        for child in children:
            siblings = [block for block in children if block is not child]
            self.assertEqual(child.family, BlockFamily(parent, siblings, []))

    def test_iterate(self):
        """
        Iterator over parent, siblings and children
        """
        parent = DummyProcessBlock()
        children = [DummyProcessBlock(parent=parent),
                    DummyProcessBlock(parent=parent),
                    DummyProcessBlock(parent=parent),]
        grandchildren = [DummyProcessBlock(parent=children[0]),
                         DummyProcessBlock(parent=children[0]),
                         DummyProcessBlock(parent=children[0]),]
        self.assertCountEqual(children[0].family,
                              chain((parent,), children[1:], grandchildren))


class TestProcessBlock(TestCase):
    """
    Test ProcessBlock's attributes and methods
    """

    def test_public_attributes(self):
        """
        Check that public attributes exist
        """
        process_block = DummyProcessBlock()
        # events exist and are correctly ordered
        self.assertTrue(hasattr(process_block, "events"))
        for event_name in ("stop", "cancel", "requeue",):
            self.assertIn(event_name, process_block.events)
            event = process_block.events[event_name]
            self.assertIsInstance(event, EventClass)
        # master event
        self.assertTrue(hasattr(process_block, "event"))
        # family is a BlockFamily
        self.assertIsInstance(process_block.family, BlockFamily)
        # objects is an instance of a multiprocessing.queues.JoinableQueue
        self.assertIsInstance(process_block.objs, JoinableQueue)
        self.assertTrue(hasattr(process_block, "logger"))

    def test_isabstract(self):
        """
        A ProcessBlock must define a `process_obj` method
        """
        self.assertRaises(TypeError, ProcessBlock)

    def test_link_blocks(self):
        """
        Link one block (parent) to two others (children)
        """
        parent = DummyProcessBlock()
        children = [DummyProcessBlock(parent=parent),
                    DummyProcessBlock(parent=parent),]

        # parent <-> child
        self.assertCountEqual(parent.family.children, children)
        for child in children:
            self.assertEqual(child.family.parent, parent)

        # sibling <-> sibling
        self.assertCountEqual(parent.family.siblings, [])
        for child in children:
            self.assertCountEqual(
                child.family.siblings,
                filter(lambda block: block is not child, children)
                )

    def test_run(self):
        """
        Run a process block

        parent --> child (--> zombie)
        """
        parent = ZombieBlock() # Zombie <= allow child to stop
        child = DummyProcessBlock(parent=parent)
        ZombieBlock(parent=child)

        child.process_obj = Mock(side_effect=lambda obj: obj + 1)

        child.start()
        self.assertTrue(child.is_alive())

        # Simulate parent producing objects
        for i in range(10):
            parent.objs.put(i, timeout=1)
            self.assertEqual(child.objs.get(timeout=1), child.process_obj(i))
        parent.objs.put(None, timeout=1)

        child.join(timeout=1)
        self.assertFalse(child.is_alive())

    def test_warning_on_obj_failure(self):
        """
        Log ProcessingError as warnings
        """
        parent = DummyProcessBlock()
        child = FailingProcessBlock(parent=parent)

        # Logging + multiprocessing cannot be easily tested -> use a thread
        child_thr = Thread(target=child.run)
        with self.assertLogs(level='WARNING') as context_manager:
            child_thr.start()
            parent.objs.put(object())

            # Stop child_thr
            parent.events["stop"].set()
            parent.objs.put(None)
            child_thr.join(timeout=1)

        self.assertFalse(child_thr.is_alive())

        # Only one message was logged and it says "test"
        self.assertEqual(context_manager.records.pop().getMessage().strip(),
                         "test")


    def test_timeout_get_obj(self):
        """
        ProcessBlock.get_obj() times out when no object is available
        """
        block = DummyProcessBlock(parent=DummyProcessBlock())
        block._poll_interval = 0
        self.assertRaises((IndexError, Empty,), block.get_obj)

    def test_timeout_publish_obj(self):
        """
        ProcessBlock.publish_obj() times out when no obj is available
        """
        block = DummyProcessBlock(queue_size=1)
        # Just to make publish_obj to store objects in the object queue
        _ = DummyProcessBlock(parent=block)
        # To speed up the test
        block._poll_interval = 0
        block.publish_obj(0)
        self.assertRaises(Full, block.publish_obj, 0)

    def test_process_no_events(self):
        """
        process_events() returns False when there is no event
        """
        block = DummyProcessBlock()
        # ProcessBlock.event is not even set
        self.assertFalse(block.process_events())
        block.event.set()
        # No event was actually processed -> return False
        self.assertFalse(block.process_events())
        # ProcessBlock.event is cleared
        self.assertFalse(block.event.is_set())

    def test_process_one_event(self):
        """
        If an event is set process_events() runs its handler
        """
        block = DummyProcessBlock()
        block.events = OrderedDict([
            ("dummy_event", Event()),
            ])
        block.event_handlers = {
            "dummy_event": Mock(return_value=False),
            }
        block.events["dummy_event"].set()
        block.event.set()
        # An event was processed -> return True
        self.assertTrue(block.process_events())
        block.event_handlers["dummy_event"].method.assert_called_once()

    def test_break_on_event(self):
        """
        process_events() breaks on event_handlers that return True
        """
        block = DummyProcessBlock()
        block.events = OrderedDict([
            ("first", Event()),
            ("second", Event()),
            ("third", Event()),
            ])
        block.event_handlers = {
            "first": Mock(return_value=False),
            "second": Mock(return_value=True),
            "third": Mock(return_value=False),
            }

        for event in block.events.values():
            event.set()
        block.event.set()

        # An event was processed -> return True
        self.assertTrue(block.process_events())

        # The first and second events were processed...
        block.event_handlers["first"].method.assert_called_once()
        block.event_handlers["second"].method.assert_called_once()
        # but not the third
        block.event_handlers["third"].method.assert_not_called()

    def test_stop_handler(self):
        """
        stop_handler() sends one 'end object' per child block
        """
        parent = DummyProcessBlock()
        children = [ZombieBlock(parent=parent),
                    ZombieBlock(parent=parent),]

        # Stop the block
        parent.events["stop"].set()
        parent.stop_handler()

        for _ in children:
            self.assertIsNone(parent.objs.get(timeout=1))

    def test_cancel_handler(self):
        """
        cancel_handler() requeues objects in _canceled_objs
        """
        block = DummyProcessBlock()
        for obj in range(10):
            block.objs.put(obj)

        # Cancel the block
        block.events["cancel"].set()
        block.cancel_handler()

        # Order is not guaranteed
        self.assertCountEqual(block._canceled_objs, range(10))

    def test_cancel_stopped_block(self):
        """
        cancel_handler() clears the cancel and the stop event

        There also is an 'end object' tailed to _canceled_objs
        """
        block = DummyProcessBlock()
        block.events["stop"].set()

        # Cancel the block
        block.events["cancel"].set()
        block.cancel_handler()

        self.assertFalse(block.events["cancel"].is_set())
        self.assertFalse(block.events["stop"].is_set())
        self.assertIsNone(block._canceled_objs[-1])

    def test_cancel_triggers_requeue(self):
        """
        cancel_handler() sets the requeue event on child blocks
        """
        parent = DummyProcessBlock()
        children = [ZombieBlock(parent=parent),
                    ZombieBlock(parent=parent),]

        parent.events["cancel"].set()
        cancel_thr = Thread(target=parent.cancel_handler)
        cancel_thr.start()

        for child in children:
            self.assertTrue(child.events["requeue"].wait(timeout=1))
            child.events["requeue"].clear()

        # Join cancel_th
        cancel_thr.join(timeout=1)
        self.assertFalse(cancel_thr.is_alive())

    def test_requeue_handler(self):
        """
        requeue_handler() requeues objects in parent's object queue
        """
        parent = DummyProcessBlock()
        child = DummyProcessBlock(parent=parent)

        for obj in range(9):
            child.objs.put(obj)
        # Do not forget the object in ProcessBlock._obj
        child._obj = 9

        requeue_thr = Thread(target=child.requeue_handler)
        child.events["requeue"].set()
        requeue_thr.start()

        # Consume objects in parent's queue for requeue_handler() to return
        objs = []
        for _ in range(10):
            objs.append(parent.objs.get(timeout=1))
            parent.objs.task_done()

        # Order is not guaranteed to be preserved
        self.assertCountEqual(objs, range(10))

        # Join requeue_thr
        requeue_thr.join(timeout=1)
        self.assertFalse(requeue_thr.is_alive())

    def test_requeue_end_obj(self):
        """
        'end object' aka None do not get requeued
        """
        parent = DummyProcessBlock()
        child = DummyProcessBlock(parent=parent)

        child.objs.put(None)

        # Requeue
        child.events["requeue"].set()
        child.requeue_handler()

        # Ensure the child's object queue is actually emptied
        self.assertEqual(parent.objs.qsize(), 0)

    def test_requeue_stopped_block(self):
        """
        requeue_handler() clears both requeue and stop events
        """
        child = DummyProcessBlock(parent=ZombieBlock())

        # Requeue
        child.events["requeue"].set()
        child.requeue_handler()

        self.assertFalse(child.events["requeue"].is_set())
        self.assertFalse(child.events["stop"].is_set())

    def test_requeue_triggers_requeue(self):
        """
        requeue_handler() sets the requeue event on child blocks
                             --> child
        (zombie -->) parent -|
                             --> child
        """
        # parent has a zombie parent to have somewhere to requeue its objects
        parent = DummyProcessBlock(parent=ZombieBlock())
        children = [ZombieBlock(parent=parent),
                    ZombieBlock(parent=parent),]

        parent.events["requeue"].set()
        requeue_thr = Thread(target=parent.requeue_handler)
        requeue_thr.start()

        for child in children:
            self.assertTrue(child.events["requeue"].wait(timeout=1))
            child.events["requeue"].clear()

        # Join requeue_thr
        requeue_thr.join(timeout=1)
        self.assertFalse(requeue_thr.is_alive())
