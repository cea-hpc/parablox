# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
Useful classes to implement tests

DummyProcessBlock:
    A noop block

FailingProcessBlock:
    A block that always raises a ProcessingError

ZombieBlock:
    A block that is not "alive"

    This is useful to ensure the parent block stores its results
    in its queue.
"""

from multiprocessing import Event
from parablox import ProcessingError, ProcessBlock


class DummyProcessBlock(ProcessBlock):
    """
    Dummy ProcessBlock subclass, just passes on its objects
    """

    def process_obj(self, obj):
        return obj


class FailingProcessBlock(ProcessBlock):
    """
    Fails every object once before processing them successfully
    """

    def __init__(self, *args, failure_msg="test", **kwargs):
        super().__init__(*args, **kwargs)
        self._failure_msg = failure_msg

    def process_obj(self, obj):
        self._obj = None # Discard the job
        raise ProcessingError(self._failure_msg)


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
