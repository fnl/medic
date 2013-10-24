import unittest

from collections import defaultdict
from datetime import date
from io import StringIO
from tempfile import TemporaryFile

from medic.orm import *
from medic.crud import _dump

DATA = [
    Section(1, 1, 'Title', 'The Title'),
    Section(1, 2, 'Abstract', 'The Abstract'),
    Descriptor(1, 1, 'd_name', True),
    Descriptor(1, 2, 'd_name', False),
    Qualifier(1, 1, 1, 'q_name', True),
    Author(1, 1, 'first'),
    Author(1, 2, 'last'),
    Identifier(1, 'ns', 'id'),
    Database(1, 'name', 'accession'),
    Chemical(1, 1, 'uid', 'name'),
    Medline(1, 'MEDLINE', 'journal', date.today()),
]

class ParserMock:
    def __init__(self, instances):
        self.instances = instances

    def parse(self, _):
        for i in self.instances:
            yield i


class TestDump(unittest.TestCase):

    def setUp(self):
        self.out = {
            Medline.__tablename__: StringIO(),
            Section.__tablename__: StringIO(),
            Descriptor.__tablename__: StringIO(),
            Qualifier.__tablename__: StringIO(),
            Author.__tablename__: StringIO(),
            Identifier.__tablename__: StringIO(),
            Database.__tablename__: StringIO(),
            Chemical.__tablename__: StringIO(),
        }

    def testDumpCount(self):
        parser = ParserMock(DATA + DATA)
        self.assertEqual(2, _dump(TemporaryFile(), self.out, parser))

    def testDumping(self):
        parser = ParserMock(DATA)
        results = defaultdict(str)

        for i in DATA:
            results[i.__tablename__] += str(i)

        self.assertEqual(1, _dump(TemporaryFile(), self.out, parser))

        for tbl, buff in self.out.items():
            self.assertEqual(results[tbl], buff.getvalue())


if __name__ == '__main__':
    unittest.main()
