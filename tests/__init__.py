# Copyright (C) 2018  quentin.bouget@cea.fr
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
