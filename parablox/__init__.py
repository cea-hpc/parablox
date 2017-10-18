# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
This package provides an interface to easily pipe Process objects

Classes exposed:

# ProcessBlock
Abstract class that representing a process

# EventInterrupt
Exception to be raised when an event interrupts a process

# ProcessingError
Exception to be raised on error in ProcessBlock.proc
"""

from parablox.processblock import EventInterrupt, ProcessingError, ProcessBlock

__all__ = ["EventInterrupt", "ProcessingError", "ProcessBlock",]
__version__ = "0.1.0"
