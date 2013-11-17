from sqlite3 import dbapi2
from os.path import dirname
from unittest import main, TestCase
from sqlalchemy.engine.url import URL
from datetime import date

from medic import orm
from medic.parser import MedlineXMLParser, PubMedXMLParser

__author__ = 'Florian Leitner'


class ParserTest(TestCase):
    MEDLINE_STRUCTURE_FILE = dirname(__file__) + '/medline.xml'
    PMID = 123
    ITEMS = [
        orm.Identifier(PMID, 'doi', 'valid doi'),
        orm.Identifier(PMID, 'pii', 'invalid pii'),
        orm.Abstract(PMID, 'NLM', 'copyright info'),
        orm.Section(PMID, 'NLM', 1, 'Background', 'background text', 'background label'),
        orm.Section(PMID, 'NLM', 2, 'Objective', 'objective text', 'objective label'),
        orm.Section(PMID, 'NLM', 3, 'Methods', 'methods text', 'methods label'),
        orm.Section(PMID, 'NLM', 4, 'Methods', 'duplicate methods', 'methods label'),
        orm.Section(PMID, 'NLM', 5, 'Results', 'results text', 'results label'),
        orm.Section(PMID, 'NLM', 6, 'Conclusions', 'conclusions text', 'conclusions label'),
        orm.Section(PMID, 'NLM', 7, 'Unlabelled', 'unlabelled text with\xa0encoding\x96errors'),
        orm.Section(PMID, 'NLM', 8, 'Unassigned', 'abstract text', 'abstract label'),
        orm.Section(PMID, 'NLM', 9, 'Abstract', 'default text'),
        orm.Author(PMID, 1, 'Author', forename='First'),
        orm.Author(PMID, 2, 'Middle', suffix='Suf'),
        orm.Author(PMID, 3, 'Author', forename='P Last', initials='PL'),
        orm.Database(PMID, 'db1', 'acc1'),
        orm.Database(PMID, 'db1', 'acc2'),
        orm.Database(PMID, 'db1', 'acc3'),
        orm.Database(PMID, 'db2', 'acc'),
        orm.PublicationType(PMID, 'publication type 1'.upper()),
        orm.PublicationType(PMID, 'publication type 2'.upper()),
        orm.Chemical(PMID, 1, "chemical substance 1"),
        orm.Chemical(PMID, 2, "chemical substance 2", "EC 1.1.1.1"),
        orm.Descriptor(PMID, 1, 'minor geographic descriptor'),
        orm.Qualifier(PMID, 1, 1, 'minor qualifier'),
        orm.Descriptor(PMID, 2, 'major descriptor', True),
        orm.Qualifier(PMID, 2, 1, 'major qualifier', True),
        orm.Qualifier(PMID, 2, 2, 'another qualifier'),
        orm.Descriptor(PMID, 3, 'major descriptor', True),
        orm.Qualifier(PMID, 3, 1, 'minor qualifier'),
        orm.Descriptor(PMID, 4, 'minor descriptor'),
        orm.Qualifier(PMID, 4, 1, 'major qualifier', True),
        orm.Identifier(PMID, 'pmc', 'PMC12345'),
        orm.Abstract(PMID, 'Publisher', None),
        orm.Section(PMID, 'Publisher', 1, 'Unassigned', 'explanation for publisher abstract'),
        orm.Abstract(PMID, 'NASA', 'NASA copyright'),
        orm.Section(PMID, 'NASA', 1, 'Conclusions', 'NASA conclusions', 'NASA label'),
        orm.Keyword(PMID, 'NLM', 1, 'NLM minor keyword'),
        orm.Keyword(PMID, 'NLM', 2, 'major keyword', True),
        orm.Keyword(PMID, 'NLM', 3, 'minor keyword'),
        orm.Keyword(PMID, 'NASA', 1, 'NASA keyword'),
        orm.Citation(PMID, 'MEDLINE', '[a translated title].', 'NLM Jour Abbrev', 'some random date string',
                    date(1974, 2, 19), date(1974, 11, 19), date(2006, 2, 14),
                    'vol 1(no 1)', '100-10'),
        orm.Citation(987, 'MEDLINE', 'title', 'NLM Jour Abbrev', 'year season',
                    date(1974, 2, 19), date(1974, 11, 19), date(2006, 2, 14)),
        orm.Identifier(987, 'doi', 'some doi'),
    ]

    def setUp(self):
        self.stream = open(ParserTest.MEDLINE_STRUCTURE_FILE)

    def testParseToDB(self):
        orm.InitDb(URL('sqlite'), module=dbapi2)
        self.sess = orm.Session()
        count = 0
        parser = PubMedXMLParser(False)

        for item in parser.parse(self.stream):
            if type(item) == int:
                assert item in (123, 987), item
            else:
                count += 1
                self.sess.add(item)

        self.assertEqual(len(ParserTest.ITEMS), count)
        self.sess.commit()

    def testParseAll(self):
        items = ParserTest.ITEMS
        parser = PubMedXMLParser(False)
        i = -1
        item = None

        for i, item in enumerate(parser.parse(self.stream)):
            if type(item) == int:
                assert item in (123, 987), item
            else:
                self.assertEqual(str(items[i]), str(item))
                self.assertEqual(items[i], item, "\n" + str(item) + str(items[i]))

        self.assertEqual(len(items) - 1, i - 2, repr(item))

    def testParseSkipVersion(self):
        items = ParserTest.ITEMS[:-2]
        parser = MedlineXMLParser()
        i = -1
        item = None

        for i, item in enumerate(parser.parse(self.stream)):
            if type(item) == int:
                assert item in (123, 987), item
            else:
                self.assertEqual(str(items[i]), str(item), str(item))
                self.assertEqual(items[i], item)

        self.assertEqual(len(items) - 1, i - 2, repr(item))


if __name__ == '__main__':
    main()
