# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
Test the BlockFamily class
"""

from itertools import chain
from unittest import TestCase

from parablox.processblock import BlockFamily
from tests import DummyProcessBlock


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
