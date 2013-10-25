from datetime import timedelta, date
from sqlite3 import dbapi2
from sqlalchemy.engine.url import URL
from unittest import main, TestCase

from sqlalchemy.exc import IntegrityError, StatementError

from medic.orm import InitDb, Session, \
        Medline, Section, Author, Descriptor, Qualifier, Database, Identifier, Chemical

__author__ = 'Florian Leitner'

URI = "sqlite+pysqlite://"  # use in-memmory SQLite DB for testing


class InitdbTest(TestCase):
    def testUsingURI(self):
        InitDb(URI, module=dbapi2)
        self.assertEqual('sqlite', Session().connection().dialect.name)

    def testUsingURL(self):
        InitDb(URL('sqlite'), module=dbapi2)
        self.assertEqual('sqlite', Session().connection().dialect.name)


class MedlineTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()

    def testCreate(self):
        r = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(r)
        self.sess.commit()
        self.assertEqual(r, self.sess.query(Medline).first())

    def testEquals(self):
        d = date.today()
        self.assertEqual(Medline(1, 'MEDLINE', 'journal', d),
                         Medline(1, 'MEDLINE', 'journal', d))

    def testAssignPmid(self):
        big = 987654321098765432  # up to 18 decimals
        r = Medline(big, 'MEDLINE', 'journal', date.today())
        self.sess.add(r)
        self.sess.commit()
        self.assertEqual(big, r.pmid)
        self.assertEqual(big, self.sess.query(Medline).first().pmid)

    def testRequireNonZeroPmid(self):
        self.assertRaises(AssertionError, Medline,
                          0, 'MEDLINE', 'journal', date.today())
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.pmid = 0
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonNegativePmid(self):
        self.assertRaises(AssertionError, Medline,
                          -1, 'MEDLINE', 'journal', date.today())
        m = Medline(2, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.pmid = -1
        self.assertRaises(IntegrityError, self.sess.commit)

    def testValidStatusNames(self):
        d = date.today()
        for n in Medline.STATES:
            Medline(1, n, 'journal', d)
        self.assertTrue(True)

    def testRequireStatus(self):
        self.assertRaises(AssertionError, Medline,
                          1, None, 'journal', date.today())
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.status = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyStatus(self):
        self.assertRaises(AssertionError, Medline,
                          1, '', 'journal', date.today())
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.status = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireValidStatus(self):
        self.assertRaises(AssertionError, Medline,
                          1, 'invalid', 'journal', date.today())
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.status = 'invalid'
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireJournal(self):
        self.assertRaises(AssertionError, Medline,
                          1, 'MEDLINE', None, date.today())
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.journal = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyJournal(self):
        self.assertRaises(AssertionError, Medline,
                          1, 'MEDLINE', '', date.today())
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.journal = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireCreated(self):
        self.assertRaises(AssertionError, Medline,
                          1, 'MEDLINE', 'journal', None)
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(m)
        m.created = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireCompletedDateOrNone(self):
        self.assertRaises(AssertionError, Medline,
                          1, 'MEDLINE', 'journal', date.today(), completed='')
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        m.completed = date.today()
        self.sess.add(m)
        self.sess.commit()
        m.completed = ''
        self.assertRaises(StatementError, self.sess.commit)

    def testRequireRevisedDateOrNone(self):
        self.assertRaises(AssertionError, Medline,
                          1, 'MEDLINE', 'journal', date.today(), revised='')
        m = Medline(1, 'MEDLINE', 'journal', date.today())
        m.revised = date.today()
        self.sess.add(m)
        self.sess.commit()
        m.revised = ''
        self.assertRaises(StatementError, self.sess.commit)

    def testAutomaticModified(self):
        r = Medline(1, 'MEDLINE', 'journal', date.today())
        self.sess.add(r)
        self.sess.commit()
        self.assertEqual(date.today(), r.modified)

    def testModifiedBefore(self):
        today = date.today()
        yesterday = today - timedelta(days=1)
        m1 = Medline(1, 'MEDLINE', 'Journal 1', today)
        m2 = Medline(2, 'MEDLINE', 'Journal 1', today)
        m1.modified = today
        m2.modified = yesterday
        self.sess.add(m1)
        self.sess.add(m2)
        self.sess.commit()
        self.assertListEqual([2], list(Medline.modifiedBefore([1, 2], date.today())))

    def testToString(self):
        d = date.today()
        r = Medline(1, 'MEDLINE', 'journal\\.', d)
        line = "1\tMEDLINE\tjournal\\.\t{}\t\\N\t\\N\t{}\n".format(d.isoformat(), d.isoformat())
        self.assertEqual(line, str(r))

    def testToRepr(self):
        r = Medline(1, 'MEDLINE', 'journal', date.today())
        self.assertEqual("Medline<1>", repr(r))

    def testInsert(self):
        data = {
            Medline.__tablename__: [
                dict(pmid=1, status='MEDLINE', journal='Journal', created=date.today())
            ],
            Section.__tablename__: [
                dict(pmid=1, seq=1, name='Title', content='The title.')
            ],
            Author.__tablename__: [
                dict(pmid=1, pos=1, name='Author')
            ],
            Descriptor.__tablename__: [
                dict(pmid=1, num=1, name='descriptor', major=True)
            ],
            Qualifier.__tablename__: [
                dict(pmid=1, num=1, sub=1, name='descriptor', major=True)
            ],
            Identifier.__tablename__: [
                dict(pmid=1, namespace='ns', value='id')
            ],
            Database.__tablename__: [
                dict(pmid=1, name='name', accession='accession')
            ],
        }
        Medline.insert(data)
        for m in self.sess.query(Medline):
            self.assertEqual(1, m.pmid)
        for t in Medline.CHILDREN:
            self.assertEqual(1, list(self.sess.query(t))[0].pmid)

    def testInsertMultiple(self):
        d = date.today()
        data = {Medline.__tablename__: [
            dict(pmid=1, status='MEDLINE', journal='Journal 1', created=d),
            dict(pmid=2, status='MEDLINE', journal='Journal 2', created=d),
            dict(pmid=3, status='MEDLINE', journal='Journal 3', created=d)
        ]}
        Medline.insert(data)
        count = 0
        for m in self.sess.query(Medline):
            if m.pmid not in (1, 2, 3):
                self.fail(m)
            else:
                count += 1
        self.assertEqual(3, count)

    def addThree(self, d):
        self.sess.add(Medline(1, 'MEDLINE', 'Journal 1', d))
        self.sess.add(Medline(2, 'MEDLINE', 'Journal 2', d))
        self.sess.add(Medline(3, 'MEDLINE', 'Journal X', d))
        self.sess.commit()

    def testSelect(self):
        self.addThree(date.today())
        count = 0
        for row in Medline.select([1, 2], ['journal']):
            self.assertEqual('Journal {}'.format(row['pmid']), row['journal'])
            count += 1
        self.assertEqual(2, count)

    def testSelectAll(self):
        d = date.today()
        self.addThree(d)
        count = 0
        for row in Medline.selectAll([1, 2]):
            self.assertEqual('Journal {}'.format(row['pmid']), row['journal'])
            self.assertEqual(d, row['created'])
            self.assertEqual('MEDLINE', row[1])
            count += 1
        self.assertEqual(2, count)

    def testDelete(self):
        self.addThree(date.today())
        Medline.delete([1, 2])
        count = 0
        for m in self.sess.query(Medline):
            self.assertEqual(3, m.pmid)
            count += 1
        self.assertEqual(1, count)

    def testExisting(self):
        self.addThree(date.today())
        self.assertListEqual([1, 3], list(Medline.existing([1, 3, 5])))


class SectionTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)

    def testCreate(self):
        a = Section(1, 1, 'Title', 'The Title')
        self.sess.add(a)
        self.sess.commit()
        self.assertEqual(a, self.sess.query(Section).first())

    def testEquals(self):
        self.assertEqual(Section(1, 1, 'Title', 'The Title'),
                         Section(1, 1, 'Title', 'The Title'))

    def testRequirePmid(self):
        self.assertRaises(TypeError, Section, None, 1, 'Title', 'The Title')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.pmid = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireExistingPmid(self):
        self.sess.add(Section(2, 1, 'Title', 'The Title'))
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireSeq(self):
        self.assertRaises(TypeError, Section, 1, None, 'Title', 'The Title')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.seq = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonZeroSeq(self):
        self.assertRaises(AssertionError, Section, 1, 0, 'Title', 'The Title')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.seq = 0
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequirePositiveSeq(self):
        self.assertRaises(AssertionError, Section, 1, -1, 'Title', 'The Title')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.seq = -1
        self.assertRaises(IntegrityError, self.sess.commit)

    def testValidSectionNames(self):
        for s in Section.SECTIONS:
            Section(1, 1, s, 'text')
        self.assertTrue(True)

    def testRequireSectionName(self):
        self.assertRaises(AssertionError, Section, 1, 1, None, 'text')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.name = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptySectionName(self):
        self.assertRaises(AssertionError, Section, 1, 1, '', 'text')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.name = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireValidSectionName(self):
        self.assertRaises(AssertionError, Section, 1, 1, 'invalid', 'text')
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        s.name = 'invalid'
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireContent(self):
        self.assertRaises(AssertionError, Section, 1, 1, 'Title', None)
        m = Section(1, 1, 'Title', 'txt')
        self.sess.add(m)
        m.content = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyContent(self):
        self.assertRaises(AssertionError, Section, 1, 1, 'Title', '')
        m = Section(1, 1, 'Title', 'txt')
        self.sess.add(m)
        m.content = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testToString(self):
        self.assertEqual('1\t1\tTitle\tlabel\t"co\\n\\tent"\\\\\n',
                         str(Section(1, 1, 'Title', "\"co\n\tent\"\\", 'label')))

    def testToRepr(self):
        self.assertEqual('Section<1:1>',
                         repr(Section(1, 1, 'Title', 'content', 'label')))

    def testMedlineRelations(self):
        title = Section(1, 1, 'Title', 'The Title')
        abstract = Section(1, 2, 'Abstract', 'The Abstract.')
        self.sess.add(title)
        self.sess.add(abstract)
        self.sess.commit()
        self.assertListEqual([title, abstract], self.M.sections)

    def testSectionRelations(self):
        s = Section(1, 1, 'Title', 'The Title')
        self.sess.add(s)
        self.sess.commit()
        self.assertEqual(self.M, s.medline)


class DescriptorTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)

    def testCreate(self):
        d = Descriptor(1, 1, 'd_name', True)
        self.sess.add(d)
        self.sess.commit()
        self.assertEqual(d, self.sess.query(Descriptor).first())

    def testEquals(self):
        self.assertEqual(Descriptor(1, 1, 'd_name', True),
                         Descriptor(1, 1, 'd_name', True))

    def testRequirePmid(self):
        self.assertRaises(TypeError, Descriptor, None, 1, 'd_name', True)
        d = Descriptor(1, 1, 'd_name', True)
        self.sess.add(d)
        d.pmid = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireExistingPmid(self):
        self.sess.add(Descriptor(2, 1, 'd_name', True))
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNum(self):
        self.assertRaises(TypeError, Descriptor, 1, None, 'd_name', True)
        d = Descriptor(1, 1, 'd_name', True)
        self.sess.add(d)
        d.num = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonZeroNum(self):
        self.assertRaises(AssertionError, Descriptor, 1, 0, 'd_name', True)
        d = Descriptor(1, 1, 'd_name', True)
        self.sess.add(d)
        d.num = 0
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonNegativeNum(self):
        self.assertRaises(AssertionError, Descriptor, 1, -1, 'd_name', True)
        d = Descriptor(1, 1, 'd_name', True)
        self.sess.add(d)
        d.num = -1
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireName(self):
        self.assertRaises(AssertionError, Descriptor, 1, 1, '', True)
        d = Descriptor(1, 1, 'd_name', True)
        self.sess.add(d)
        d.name = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireMajor(self):
        # noinspection PyTypeChecker
        self.sess.add(Descriptor(1, 1, 'd_name', None))
        self.assertRaises(IntegrityError, self.sess.commit)

    def testToString(self):
        self.assertEqual('1\t1\tmajor\tT\n', str(Descriptor(1, 1, 'major', True)))
        self.assertEqual('1\t1\tminor\tF\n', str(Descriptor(1, 1, 'minor', False)))

    def testToRepr(self):
        self.assertEqual('Descriptor<1:2>', repr(Descriptor(1, 2, 'name', True)))

    def testMedlineRelations(self):
        d = Descriptor(1, 1, 'name', True)
        self.sess.add(d)
        self.sess.commit()
        self.assertListEqual([d], self.M.descriptors)

    def testDescriptorRelations(self):
        d = Descriptor(1, 1, 'name', True)
        self.sess.add(d)
        self.sess.commit()
        self.assertEqual(self.M, d.medline)

    def testSelect(self):
        self.sess.add(Descriptor(1, 1, 'd1', True))
        self.sess.add(Descriptor(1, 2, 'd2', False))
        self.sess.add(Descriptor(1, 3, 'd3', True))
        self.sess.commit()
        for row in Descriptor.select(1, ['num', 'name']):
            if row['num'] == 1:
                self.assertEqual(row['name'], 'd1')
            elif row[0] == 2:
                self.assertEqual(row[1], 'd2')
            elif row[0] == 3:
                self.assertEqual(row[1], 'd3')
            else:
                self.fail(str(row))

    def testSelectAll(self):
        self.sess.add(Descriptor(1, 1, 'd1', True))
        self.sess.add(Descriptor(1, 2, 'd2', False))
        self.sess.add(Descriptor(1, 3, 'd3', True))
        self.sess.commit()
        for row in Descriptor.selectAll(1):
            if row['num'] == 1:
                self.assertEqual(row['name'], 'd1')
            elif row[1] == 2:
                # noinspection PyUnresolvedReferences
                self.assertFalse(row[Descriptor.major.name])
            elif row[1] == 3:
                self.assertEqual(row[2], 'd3')
            else:
                self.fail(str(row))


class QualifierTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)
        self.D = Descriptor(1, 1, 'd_name', True)
        self.sess.add(self.D)

    def testCreate(self):
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        self.sess.commit()
        self.assertEqual(q, self.sess.query(Qualifier).first())

    def testEquals(self):
        self.assertEqual(Qualifier(1, 1, 1, 'q_name', True),
                         Qualifier(1, 1, 1, 'q_name', True))

    def testRequirePmid(self):
        self.assertRaises(TypeError, Qualifier, None, 1, 1, 'q_name', True)
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        q.pmid = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNum(self):
        self.assertRaises(TypeError, Qualifier, 1, None, 1, 'q_name', True)
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        q.num = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireExistingDescriptor(self):
        self.sess.add(Qualifier(1, 2, 1, 'q_name', True))
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireSub(self):
        self.assertRaises(TypeError, Qualifier, 1, 1, None, 'q_name', True)
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        q.sub = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonZeroSub(self):
        self.assertRaises(AssertionError, Qualifier, 1, 1, 0, 'q_name', True)
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        q.sub = 0
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonNegativeSub(self):
        self.assertRaises(AssertionError, Qualifier, 1, 1, -1, 'q_name', True)
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        q.sub = -1
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireName(self):
        self.assertRaises(AssertionError, Qualifier, 1, 1, 1, '', True)
        q = Qualifier(1, 1, 1, 'q_name', True)
        self.sess.add(q)
        q.name = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireMajor(self):
        # noinspection PyTypeChecker
        self.sess.add(Qualifier(1, 1, 1, 'q_name', None))
        self.assertRaises(IntegrityError, self.sess.commit)

    def testToString(self):
        self.assertEqual('1\t1\t1\tmajor\tT\n', str(Qualifier(1, 1, 1, 'major', True)))
        self.assertEqual('1\t1\t1\tminor\tF\n', str(Qualifier(1, 1, 1, 'minor', False)))

    def testToRepr(self):
        self.assertEqual('Qualifier<1:2:3>', repr(Qualifier(1, 2, 3, 'name', True)))

    def testMedlineRelations(self):
        q = Qualifier(1, 1, 1, 'name', True)
        self.sess.add(q)
        self.sess.commit()
        self.assertListEqual([q], self.M.qualifiers)

    def testQualifierRelations(self):
        q = Qualifier(1, 1, 1, 'name', True)
        self.sess.add(q)
        self.sess.commit()
        self.assertEqual(self.M, q.medline)
        self.assertEqual(self.D, q.descriptor)

    def testSelect(self):
        self.sess.add(Qualifier(1, 1, 1, 'q1', True))
        self.sess.add(Qualifier(1, 1, 2, 'q2', False))
        self.sess.add(Qualifier(1, 1, 3, 'q3', True))
        self.sess.commit()
        for row in Qualifier.select((1, 1), ['sub', 'name']):
            if row['sub'] == 1:
                self.assertEqual(row['name'], 'q1')
            elif row[0] == 2:
                self.assertEqual(row[1], 'q2')
            elif row[0] == 3:
                self.assertEqual(row[1], 'q3')
            else:
                self.fail(str(row))

    def testSelectAll(self):
        self.sess.add(Qualifier(1, 1, 1, 'q1', True))
        self.sess.add(Qualifier(1, 1, 2, 'q2', False))
        self.sess.add(Qualifier(1, 1, 3, 'q3', True))
        self.sess.commit()
        for row in Qualifier.selectAll((1, 1)):
            if row['sub'] == 1:
                self.assertEqual(row['name'], 'q1')
            elif row[2] == 2:
                # noinspection PyUnresolvedReferences
                self.assertFalse(row[Qualifier.major.name])
            elif row[2] == 3:
                self.assertEqual(row[3], 'q3')
            else:
                self.fail(str(row))


class AuthorTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)

    def testCreate(self):
        a = Author(1, 1, 'last')
        self.sess.add(a)
        self.sess.commit()
        self.assertEqual(a, self.sess.query(Author).first())

    def testEquals(self):
        self.assertEqual(Author(1, 1, 'last'),
                         Author(1, 1, 'last'))

    def testRequirePmid(self):
        self.assertRaises(TypeError, Author, None, 1, 'last')
        a = Author(1, 1, 'last')
        self.sess.add(a)
        a.pmid = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequirePos(self):
        self.assertRaises(TypeError, Author, 1, None, 'last')
        a = Author(1, 1, 'last')
        self.sess.add(a)
        a.pos = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonZeroPos(self):
        self.assertRaises(AssertionError, Author, 1, 0, 'last')
        a = Author(1, 1, 'last')
        self.sess.add(a)
        a.pos = 0
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonNegativePos(self):
        self.assertRaises(AssertionError, Author, 1, -1, 'last')
        a = Author(1, 1, 'last')
        self.sess.add(a)
        a.pos = -1
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireName(self):
        self.assertRaises(AssertionError, Author, 1, 1, None)
        a = Author(1, 1, 'last')
        self.sess.add(a)
        a.name = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyName(self):
        self.assertRaises(AssertionError, Author, 1, 1, '')
        a = Author(1, 1, 'last')
        self.sess.add(a)
        a.name = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testAssignInitials(self):
        self.sess.add(Author(1, 1, 'last', initials='test'))
        self.assertEqual('test', self.sess.query(Author).first().initials)

    def testAssignFirstName(self):
        self.sess.add(Author(1, 1, 'last', forename='test'))
        self.assertEqual('test', self.sess.query(Author).first().forename)

    def testAssignSuffix(self):
        self.sess.add(Author(1, 1, 'last', suffix='test'))
        self.assertEqual('test', self.sess.query(Author).first().suffix)

    def testFullNameDefault(self):
        a = Author(1, 1, 'last', initials='init', forename='first', suffix='suffix')
        self.assertEqual('first last suffix', a.fullName())

    def testFullNameNoFirst(self):
        a = Author(1, 1, 'last', initials='init', forename='', suffix='suffix')
        self.assertEqual('init last suffix', a.fullName())

    def testFullNameNoFirstOrInitials(self):
        a = Author(1, 1, 'last', initials='', forename='', suffix='suffix')
        self.assertEqual('last suffix', a.fullName())

    def testFullNameNoSuffix(self):
        a = Author(1, 1, 'last', initials='init', forename='first', suffix='')
        self.assertEqual('first last', a.fullName())

    def testShortNameDefault(self):
        a = Author(1, 1, 'last', initials='init', forename='first', suffix='suffix')
        self.assertEqual('init last', a.shortName())

    def testShortNameNoInitials(self):
        a = Author(1, 1, 'last', initials='', forename='first second', suffix='suffix')
        self.assertEqual('fs last', a.shortName())

    def testShortNameNoFirstOrInitials(self):
        a = Author(1, 1, 'last', initials='', forename='', suffix='suffix')
        self.assertEqual('last', a.shortName())

    def testToString(self):
        self.assertEqual('1\t1\tlast\t\\N\tfirst\t\n',
                         str(Author(1, 1, 'last', forename='first', suffix='')))

    def testToRepr(self):
        self.assertEqual('Author<1:1>', repr(Author(1, 1, 'name')))

    def testMedlineRelations(self):
        a = Author(1, 1, 'last')
        self.sess.add(a)
        self.sess.commit()
        self.assertListEqual([a], self.M.authors)

    def testAuthorRelations(self):
        a = Author(1, 1, 'last')
        self.sess.add(a)
        self.sess.commit()
        self.assertEqual(self.M, a.medline)


class IdentifierTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)

    def testCreate(self):
        i = Identifier(1, 'ns', 'id')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual(i, self.sess.query(Identifier).first())

    def testEquals(self):
        self.assertEqual(Identifier(1, 'ns', 'id'),
                         Identifier(1, 'ns', 'id'))

    def testRequireNamespace(self):
        self.assertRaises(AssertionError, Identifier, 1, None, 'id')
        m = Identifier(1, 'ns', 'id')
        self.sess.add(m)
        m.namespace = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyNamespace(self):
        self.assertRaises(AssertionError, Identifier, 1, '', 'id')
        m = Identifier(1, 'ns', 'id')
        self.sess.add(m)
        m.namespace = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireAccession(self):
        self.assertRaises(AssertionError, Identifier, 1, 'ns', None)
        m = Identifier(1, 'ns', 'id')
        self.sess.add(m)
        m.value = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyAccession(self):
        self.assertRaises(AssertionError, Identifier, 1, 'ns', '')
        m = Identifier(1, 'ns', 'id')
        self.sess.add(m)
        m.value = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testToString(self):
        self.assertEqual('1\tns\tid\n', str(Identifier(1, 'ns', 'id')))

    def testToRepr(self):
        self.assertEqual('Identifier<1:ns>', repr(Identifier(1, 'ns', 'id')))

    def testMedlineRelations(self):
        i = Identifier(1, 'ns', 'id')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual({'ns': i}, self.M.identifiers)

    def testIdentifierRelations(self):
        i = Identifier(1, 'ns', 'id')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual(self.M, i.medline)

    def testPmid2Doi(self):
        self.sess.add(Identifier(1, 'doi', 'id'))
        self.sess.add(Medline(2, 'MEDLINE', 'Journal', date.today()))
        self.sess.commit()
        self.assertEqual('id', Identifier.pmid2doi(1))
        self.assertEqual(None, Identifier.pmid2doi(2))

    def testDoi2Pmid(self):
        self.sess.add(Identifier(1, 'doi', 'id'))
        self.sess.add(Medline(2, 'MEDLINE', 'Journal', date.today()))
        self.sess.commit()
        self.assertEqual(1, Identifier.doi2pmid('id'))
        self.assertEqual(None, Identifier.doi2pmid('other'))

    def testMapPmids2Dois(self):
        self.sess.add(Identifier(1, 'doi', 'id1'))
        self.sess.add(Medline(2, 'MEDLINE', 'Journal', date.today()))
        self.sess.add(Identifier(2, 'doi', 'id2'))
        self.sess.commit()
        self.assertDictEqual({1: 'id1', 2: 'id2'}, Identifier.mapPmids2Dois([1, 2, 3]))
        self.assertDictEqual({}, Identifier.mapPmids2Dois([3, 4]))

    def testMapDois2Pmids(self):
        self.sess.add(Identifier(1, 'doi', 'id1'))
        self.sess.add(Medline(2, 'MEDLINE', 'Journal', date.today()))
        self.sess.add(Identifier(2, 'doi', 'id2'))
        self.sess.commit()
        self.assertDictEqual({'id1': 1, 'id2': 2},
                             Identifier.mapDois2Pmids(['id1', 'id2', 'id3']))
        self.assertDictEqual({}, Identifier.mapDois2Pmids(['id3', 'id4']))


class DatabaseTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)

    def testCreate(self):
        i = Database(1, 'name', 'accession')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual(i, self.sess.query(Database).first())

    def testEquals(self):
        self.assertEqual(Database(1, 'name', 'accession'),
                         Database(1, 'name', 'accession'))

    def testRequireName(self):
        self.assertRaises(AssertionError, Database, 1, None, 'accession')
        m = Database(1, 'name', 'accession')
        self.sess.add(m)
        m.name = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyName(self):
        self.assertRaises(AssertionError, Database, 1, '', 'accession')
        db = Database(1, 'name', 'accession')
        self.sess.add(db)
        db.name = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireAccession(self):
        self.assertRaises(AssertionError, Database, 1, 'name', None)
        m = Database(1, 'name', 'accession')
        self.sess.add(m)
        m.accession = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireNonEmptyAccession(self):
        self.assertRaises(AssertionError, Database, 1, 'name', '')
        m = Database(1, 'name', 'accession')
        self.sess.add(m)
        m.accession = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testToString(self):
        self.assertEqual('1\tname\taccession\n', str(Database(1, 'name', 'accession')))

    def testToRepr(self):
        self.assertEqual('Database<1:name:accession>', repr(Database(1, 'name', 'accession')))

    def testMedlineRelations(self):
        i = Database(1, 'name', 'accession')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual([i], self.M.databases)

    def testIdentifierRelations(self):
        i = Database(1, 'name', 'accession')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual(self.M, i.medline)


class ChemicalTest(TestCase):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = Medline(1, 'MEDLINE', 'Journal', date.today())
        self.sess.add(self.M)

    def testCreate(self):
        i = Chemical(1, 1, 'uid', 'name')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual(i, self.sess.query(Chemical).first())

    def testEquals(self):
        self.assertEqual(Chemical(1, 1, 'uid', 'name'),
                         Chemical(1, 1, 'uid', 'name'))

    def testUidNotRequired(self):
        self.assertEqual(Chemical(1, 1, '', 'name'),
                         Chemical(1, 1, '', 'name'))
        self.assertEqual(Chemical(1, 1, None, 'name'),
                         Chemical(1, 1, None, 'name'))

    def testRequireNonEmptyName(self):
        self.assertRaises(AssertionError, Chemical, 1, 1, 'uid', '')
        db = Chemical(1, 1, 'uid', 'name')
        self.sess.add(db)
        db.name = ''
        self.assertRaises(IntegrityError, self.sess.commit)

    def testRequireName(self):
        self.assertRaises(AssertionError, Chemical, 1, 1, 'uid', None)
        m = Chemical(1, 1, 'uid', 'name')
        self.sess.add(m)
        m.name = None
        self.assertRaises(IntegrityError, self.sess.commit)

    def testToString(self):
        self.assertEqual('1\t1\tuid\tname\n', str(Chemical(1, 1, 'uid', 'name')))

    def testToRepr(self):
        self.assertEqual('Chemical<1:1>', repr(Chemical(1, 1, None, 'name')))

    def testMedlineRelations(self):
        i = Chemical(1, 1, 'uid', 'name')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual([i], self.M.chemicals)

    def testIdentifierRelations(self):
        i = Chemical(1, 1, 'uid', 'name')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual(self.M, i.medline)


if __name__ == '__main__':
    main()
