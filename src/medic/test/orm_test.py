from collections import namedtuple
from datetime import timedelta, date
from sqlite3 import dbapi2
from sqlalchemy.engine.url import URL
from unittest import main, TestCase

from sqlalchemy.exc import IntegrityError, StatementError

from medic.orm import InitDb, Session, Citation, Section, Author, Descriptor, Qualifier, \
    Database, Identifier, Chemical, Keyword, PublicationType, Abstract

__author__ = 'Florian Leitner'

URI = "sqlite+pysqlite://"  # use in-memmory SQLite DB for testing


def DefaultCitation(pmid=1, status='MEDLINE', title='title',
                    journal='journal', pub_date='published', created=date.today()):
    return Citation(pmid, status, title, journal, pub_date, created)


class InitdbTest(TestCase):
    def testUsingURI(self):
        InitDb(URI, module=dbapi2)
        self.assertEqual('sqlite', Session().connection().dialect.name)

    def testUsingURL(self):
        InitDb(URL('sqlite'), module=dbapi2)
        self.assertEqual('sqlite', Session().connection().dialect.name)


# noinspection PyUnresolvedReferences
class TestMixin:
    def assertBadExtraValue(self, error, field, value):
        self.assertRaises(error, self.klass, *self.defaults, **{field: value})
        instance = self.klass(*self.defaults)
        self.sess.add(instance)
        setattr(instance, field, value)
        self.assertRaises(StatementError, self.sess.commit)

    def assertBadValue(self, value, error, field):
        # noinspection PyProtectedMember
        bad_values = self.defaults._replace(**{field: value})
        self.assertRaises(error, self.klass, *bad_values)
        instance = self.klass(*self.defaults)
        self.sess.add(instance)
        setattr(instance, field, value)
        self.assertRaises(IntegrityError, self.sess.commit)

    def assertNonNullValue(self, error, field):
        self.assertBadValue(None, error, field)

    def assertNonEmptyValue(self, error, field):
        self.assertBadValue('', error, field)

    def assertPositiveValue(self, error, field):
        self.assertBadValue(0, error, field)
        self.sess.rollback()
        self.assertBadValue(-1, error, field)

    def assertSame(self):
        self.assertEqual(self.klass(*self.entity(*self.defaults)),
                         self.klass(*self.entity(*self.defaults)))

    def assertCreate(self):
        instance = self.klass(*self.defaults)
        self.sess.add(instance)
        self.sess.commit()
        self.assertEqual(instance, self.sess.query(self.klass).first())

    def assertDifference(self, **values):
        # noinspection PyProtectedMember
        self.assertNotEqual(self.klass(*self.defaults._replace(**values)),
                            self.klass(*self.defaults))


class CitationTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.klass = Citation
        self.entity = namedtuple('Citation', 'pmid status title journal pub_date created')
        self.defaults = self.entity(1, 'MEDLINE', 'title', 'journal', 'published', date.today())

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(pmid=2)
        self.assertDifference(status='In-Data-Review')
        self.assertDifference(journal='other')
        self.assertDifference(pub_date='other')

    def testBigPmids(self):
        big = 987654321098765432  # up to 18 decimals
        r = DefaultCitation(big)
        self.sess.add(r)
        self.sess.commit()
        self.assertEqual(big, r.pmid)
        self.assertEqual(big, self.sess.query(Citation).first().pmid)

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    def testRequirePositivePmid(self):
        self.assertPositiveValue(AssertionError, 'pmid')

    # STATUS

    def testRequireStatus(self):
        self.assertNonNullValue(AssertionError, 'status')

    def testRequireValidStatus(self):
        self.assertBadValue('invalid', AssertionError, 'status')

    def testValidStatusNames(self):
        for pmid, status in enumerate(Citation.STATES):
            self.sess.add(DefaultCitation(pmid + 1, status))
            self.sess.commit()

        self.assertTrue(True)

    # JOURNAL

    def testRequireJournal(self):
        self.assertNonNullValue(AssertionError, 'journal')

    def testRequireNonEmptyJournal(self):
        self.assertNonEmptyValue(AssertionError, 'journal')

    # CREATED

    def testRequireCreated(self):
        self.assertNonNullValue(AssertionError, 'created')

    # COMPLETED

    def testRequireCompletedDateOrNone(self):
        self.assertBadExtraValue(AssertionError, 'completed', '')

    # REVISED

    def testRequireRevisedDateOrNone(self):
        self.assertBadExtraValue(AssertionError, 'revised', '')

    # MODIFIED

    def testRequireModifiedDateOrNone(self):
        m = Citation(*self.defaults)
        m.modified = ''
        self.sess.add(m)
        self.assertRaises(StatementError, self.sess.commit)

    def testAutomaticModified(self):
        r = DefaultCitation()
        self.sess.add(r)
        self.sess.commit()
        self.assertEqual(date.today(), r.modified)

    def testModifiedBefore(self):
        today = date.today()
        yesterday = today - timedelta(days=1)
        m1 = DefaultCitation()
        m2 = DefaultCitation(2)
        m1.modified = today
        m2.modified = yesterday
        self.sess.add(m1)
        self.sess.add(m2)
        self.sess.commit()
        self.assertListEqual([2], list(Citation.modifiedBefore([1, 2], date.today())))

    # METHODS

    def testToString(self):
        d = date.today()
        r = Citation(1, 'MEDLINE', 'title', 'journal\\.', 'PubDate', d)
        line = "1\tMEDLINE\ttitle\tjournal\\\\.\tPubDate\t\\N\t\\N\t{}\t\\N\t\\N\t{}\n".format(
            d.isoformat(), d.isoformat()
        )
        self.assertEqual(line, str(r))

    def testToRepr(self):
        r = DefaultCitation(123)
        self.assertEqual("Citation<123>", repr(r))

    def testInsert(self):
        data = {
            Citation.__tablename__: [
                dict(pmid=1, status='MEDLINE', title='Title', journal='Journal',
                     pub_date='PubDate', created=date.today())
            ],
            Abstract.__tablename__: [
                dict(pmid=1, source='NLM')
            ],
            Section.__tablename__: [
                dict(pmid=1, source='NLM', seq=1, name='Title', content='The title.')
            ],
            Author.__tablename__: [
                dict(pmid=1, pos=1, name='Author')
            ],
            Descriptor.__tablename__: [
                dict(pmid=1, num=1, major=True, name='descriptor')
            ],
            Qualifier.__tablename__: [
                dict(pmid=1, num=1, sub=1, major=True, name='descriptor')
            ],
            Identifier.__tablename__: [
                dict(pmid=1, namespace='ns', value='id')
            ],
            Database.__tablename__: [
                dict(pmid=1, name='name', accession='accession')
            ],
            Chemical.__tablename__: [
                dict(pmid=1, idx=1, name='name')
            ],
            Keyword.__tablename__: [
                dict(pmid=1, owner='NLM', cnt=1, major=True, value='name')
            ],
        }
        Citation.insert(data)

        for m in self.sess.query(Citation):
            self.assertEqual(1, m.pmid)

        for t in Citation.CHILDREN:
            self.assertEqual(1, list(self.sess.query(t))[0].pmid)

    def testInsertMultiple(self):
        d = date.today()
        data = {Citation.__tablename__: [
            dict(pmid=1, status='MEDLINE', title='Title 1',
                 journal='Journal 1', pub_date='PubDate', created=d),
            dict(pmid=2, status='MEDLINE', title='Title 2',
                 journal='Journal 2', pub_date='PubDate', created=d),
            dict(pmid=3, status='MEDLINE', title='Title 3',
                 journal='Journal 3', pub_date='PubDate', created=d)
        ]}
        Citation.insert(data)
        count = 0

        for m in self.sess.query(Citation):
            if m.pmid not in (1, 2, 3):
                self.fail(m)
            else:
                count += 1

        self.assertEqual(3, count)

    def addThree(self, d):
        self.sess.add(DefaultCitation(1, journal='Journal 1', created=d))
        self.sess.add(DefaultCitation(2, journal='Journal 2', created=d))
        self.sess.add(DefaultCitation(3, created=d))
        self.sess.commit()

    def testSelect(self):
        self.addThree(date.today())
        count = 0
        for row in Citation.select([1, 2], ['journal']):
            self.assertEqual('Journal {}'.format(row['pmid']), row['journal'])
            count += 1
        self.assertEqual(2, count)

    def testSelectAll(self):
        d = date.today()
        self.addThree(d)
        count = 0
        for row in Citation.selectAll([1, 2]):
            self.assertEqual('Journal {}'.format(row['pmid']), row['journal'])
            self.assertEqual(d, row['created'])
            self.assertEqual('MEDLINE', row[1])
            count += 1
        self.assertEqual(2, count)

    def testDelete(self):
        self.addThree(date.today())
        Citation.delete([1, 2])
        count = 0
        for m in self.sess.query(Citation):
            self.assertEqual(3, m.pmid)
            count += 1
        self.assertEqual(1, count)

    def testExisting(self):
        self.addThree(date.today())
        self.assertListEqual([1, 3], list(Citation.existing([1, 3, 5])))


class SectionTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.sess.add(Abstract(1, 'NLM'))
        self.klass = Section
        self.entity = namedtuple('Section', 'pmid source seq name content')
        self.defaults = self.entity(1, 'NLM', 1, 'Title', 'The Title')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(content='other')
        self.assertDifference(name='other')
        self.assertDifference(seq=2)
        self.assertDifference(source='Publisher')

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    def testRequireExistingPmid(self):
        self.sess.add(Section(2, 'NLM', 1, 'Title', 'The Title'))
        self.assertRaises(IntegrityError, self.sess.commit)

    # SEQ

    def testRequireSeq(self):
        self.assertNonNullValue(TypeError, 'seq')

    def testRequirePositiveSeq(self):
        self.assertPositiveValue(AssertionError, 'seq')

    # NAME

    def testRequireSectionName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptySectionName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # CONTENT

    def testRequireContent(self):
        self.assertNonNullValue(AssertionError, 'content')

    def testRequireNonEmptyContent(self):
        self.assertNonEmptyValue(AssertionError, 'content')

    # LABEL

    def testNonEmptyLabel(self):
        self.assertBadExtraValue(AssertionError, 'label', '')

    # METHODS

    def testToString(self):
        self.assertEqual('1\tNLM\t1\tTitle\tlabel\t"co\\n\\tent"\\\\\tF\n',
                         str(Section(1, 'NLM', 1, 'Title', "\"co\n\tent\"\\", 'label')))

    def testToRepr(self):
        self.assertEqual('Section<1:NLM:1>',
                         repr(Section(1, 'NLM', 1, 'Title', 'content', 'label')))

    def testCitation(self):
        section = Section(1, 'NLM', 1, 'Abstract', 'The Abstract.')
        self.sess.add(section)
        self.sess.commit()
        self.assertListEqual([section], self.M.sections)
        self.assertEqual(self.M, section.citation)


class DescriptorTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = Descriptor
        self.entity = namedtuple('Descriptor', 'pmid num name')
        self.defaults = self.entity(1, 1, 'name')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(name='other')
        self.assertDifference(num=2)
        self.assertNotEqual(Descriptor(1, 1, 'd_name', True),
                            Descriptor(1, 1, 'd_name', False))

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    def testRequireExistingPmid(self):
        self.sess.add(Descriptor(2, 1, 'd_name'))
        self.assertRaises(IntegrityError, self.sess.commit)

    # NUM

    def testRequireNum(self):
        self.assertNonNullValue(TypeError, 'num')

    def testRequirePositiveNum(self):
        self.assertPositiveValue(AssertionError, 'num')

    # NAME

    def testRequireName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptyName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # MAJOR

    def testMajorIsBool(self):
        self.sess.add(Descriptor(1, 1, 'd_name', None))
        self.assertRaises(IntegrityError, self.sess.commit)

    # METHODS

    def testToString(self):
        self.assertEqual('1\t1\tT\tmajor\n', str(Descriptor(1, 1, 'major', True)))
        self.assertEqual('1\t1\tF\tminor\n', str(Descriptor(1, 1, 'minor')))

    def testToRepr(self):
        self.assertEqual('Descriptor<1:2>', repr(Descriptor(1, 2, 'name')))

    def testRelations(self):
        d = Descriptor(1, 1, 'name')
        self.sess.add(d)
        self.sess.commit()
        self.assertListEqual([d], self.M.descriptors)
        self.assertEqual(self.M, d.citation)

    def testSelect(self):
        self.sess.add(Descriptor(1, 1, 'd1'))
        self.sess.add(Descriptor(1, 2, 'd2'))
        self.sess.add(Descriptor(1, 3, 'd3'))
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
        self.sess.add(Descriptor(1, 1, 'd1'))
        self.sess.add(Descriptor(1, 2, 'd2'))
        self.sess.add(Descriptor(1, 3, 'd3'))
        self.sess.commit()

        for row in Descriptor.selectAll(1):
            if row['num'] == 1:
                self.assertEqual(row['name'], 'd1')
            elif row['num'] == 2:
                # noinspection PyUnresolvedReferences
                self.assertFalse(row[Descriptor.major.name])
            elif row['num'] == 3:
                self.assertEqual(row['name'], 'd3')
            else:
                self.fail(str(row))


class QualifierTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.D = Descriptor(1, 1, 'd_name', True)
        self.sess.add(self.D)
        self.klass = Qualifier
        self.entity = namedtuple('Qualifier', 'pmid num sub name')
        self.defaults = self.entity(1, 1, 1, 'name')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(num=2)
        self.assertDifference(sub=2)
        self.assertDifference(name='other')
        self.assertNotEqual(Qualifier(1, 1, 1, 'q_name', True),
                            Qualifier(1, 1, 1, 'q_name', False))

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # NUM

    def testRequireNum(self):
        self.assertNonNullValue(TypeError, 'num')

    def testRequireExistingDescriptor(self):
        self.sess.add(Qualifier(1, 2, 1, 'q_name'))
        self.assertRaises(IntegrityError, self.sess.commit)

    # SUB

    def testRequireSub(self):
        self.assertNonNullValue(TypeError, 'sub')

    def testRequirePositiveSub(self):
        self.assertPositiveValue(AssertionError, 'sub')

    # NAME

    def testRequireName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptyName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # MAJOR

    def testMajorIsBool(self):
        self.sess.add(Qualifier(1, 1, 1, 'q_name', None))
        self.assertRaises(IntegrityError, self.sess.commit)

    # METHODS

    def testToString(self):
        self.assertEqual('1\t1\t1\tT\tmajor\n', str(Qualifier(1, 1, 1, 'major', True)))
        self.assertEqual('1\t1\t1\tF\tminor\n', str(Qualifier(1, 1, 1, 'minor')))

    def testToRepr(self):
        self.assertEqual('Qualifier<1:2:3>', repr(Qualifier(1, 2, 3, 'name')))

    def testRelations(self):
        q = Qualifier(1, 1, 1, 'name')
        self.sess.add(q)
        self.sess.commit()
        self.assertListEqual([q], self.M.qualifiers)
        self.assertEqual(self.M, q.citation)
        self.assertEqual(self.D, q.descriptor)

    def testSelect(self):
        self.sess.add(Qualifier(1, 1, 1, 'q1'))
        self.sess.add(Qualifier(1, 1, 2, 'q2'))
        self.sess.add(Qualifier(1, 1, 3, 'q3'))
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
        self.sess.add(Qualifier(1, 1, 1, 'q1'))
        self.sess.add(Qualifier(1, 1, 2, 'q2'))
        self.sess.add(Qualifier(1, 1, 3, 'q3'))
        self.sess.commit()

        for row in Qualifier.selectAll((1, 1)):
            if row['sub'] == 1:
                self.assertEqual(row['name'], 'q1')
            elif row['sub'] == 2:
                self.assertFalse(row[Qualifier.major.name])
            elif row['sub'] == 3:
                self.assertEqual(row['name'], 'q3')
            else:
                self.fail(str(row))


class AuthorTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = Author
        self.entity = namedtuple('Author', 'pmid pos name')
        self.defaults = self.entity(1, 1, 'last')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(pos=2)
        self.assertDifference(name='other')

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # POS

    def testRequirePos(self):
        self.assertNonNullValue(TypeError, 'pos')

    def testRequirePositivePos(self):
        self.assertPositiveValue(AssertionError, 'pos')

    # NAME

    def testRequireName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptyName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # INITIALS

    def testAssignInitials(self):
        self.sess.add(Author(1, 1, 'last', initials='test'))
        self.assertEqual('test', self.sess.query(Author).first().initials)

    # FORENAME

    def testAssignFirstName(self):
        self.sess.add(Author(1, 1, 'last', forename='test'))
        self.assertEqual('test', self.sess.query(Author).first().forename)

    # SUFFIX

    def testAssignSuffix(self):
        self.sess.add(Author(1, 1, 'last', suffix='test'))
        self.assertEqual('test', self.sess.query(Author).first().suffix)

    # METHODS

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

    def testRelations(self):
        a = Author(1, 1, 'last')
        self.sess.add(a)
        self.sess.commit()
        self.assertListEqual([a], self.M.authors)
        self.assertEqual(self.M, a.citation)


class IdentifierTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = Identifier
        self.entity = namedtuple('Identifier', 'pmid namespace value')
        self.defaults = self.entity(1, 'ns', 'id')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(namespace='other')
        self.assertDifference(value='other')

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # NAMESPACE

    def testRequireNamespace(self):
        self.assertNonNullValue(AssertionError, 'namespace')

    def testRequireNonEmptyNamespace(self):
        self.assertNonEmptyValue(AssertionError, 'namespace')

    # VALUE

    def testRequireIdentifierValue(self):
        self.assertNonNullValue(AssertionError, 'value')

    def testRequireNonEmptyIdentifierValue(self):
        self.assertNonEmptyValue(AssertionError, 'value')

    # METHODS

    def testToString(self):
        self.assertEqual('1\tns\tid\n', str(Identifier(1, 'ns', 'id')))

    def testToRepr(self):
        self.assertEqual('Identifier<1:ns>', repr(Identifier(1, 'ns', 'id')))

    def testRelations(self):
        i = Identifier(1, 'ns', 'id')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual({'ns': i}, self.M.identifiers)
        self.assertEqual(self.M, i.citation)

    def testPmid2Doi(self):
        self.sess.add(Identifier(1, 'doi', 'id'))
        self.sess.add(DefaultCitation(2))
        self.sess.commit()
        self.assertEqual('id', Identifier.pmid2doi(1))
        self.assertEqual(None, Identifier.pmid2doi(2))

    def testDoi2Pmid(self):
        self.sess.add(Identifier(1, 'doi', 'id'))
        self.sess.add(DefaultCitation(2))
        self.sess.commit()
        self.assertEqual(1, Identifier.doi2pmid('id'))
        self.assertEqual(None, Identifier.doi2pmid('other'))

    def testMapPmids2Dois(self):
        self.sess.add(Identifier(1, 'doi', 'id1'))
        self.sess.add(DefaultCitation(2))
        self.sess.add(Identifier(2, 'doi', 'id2'))
        self.sess.commit()
        self.assertDictEqual({1: 'id1', 2: 'id2'}, Identifier.mapPmids2Dois([1, 2, 3]))
        self.assertDictEqual({}, Identifier.mapPmids2Dois([3, 4]))

    def testMapDois2Pmids(self):
        self.sess.add(Identifier(1, 'doi', 'id1'))
        self.sess.add(DefaultCitation(2))
        self.sess.add(Identifier(2, 'doi', 'id2'))
        self.sess.commit()
        self.assertDictEqual({'id1': 1, 'id2': 2},
                             Identifier.mapDois2Pmids(['id1', 'id2', 'id3']))
        self.assertDictEqual({}, Identifier.mapDois2Pmids(['id3', 'id4']))


class PublicationTypeTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = PublicationType
        self.entity = namedtuple('PublicationType', 'pmid value')
        self.defaults = self.entity(1, 'type')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(value='other')

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # VALUE

    def testRequireValue(self):
        self.assertNonNullValue(AssertionError, 'value')

    def testRequireNonEmptyValue(self):
        self.assertNonEmptyValue(AssertionError, 'value')

    # METHODS

    def testToString(self):
        self.assertEqual('1\ttype\n', str(PublicationType(1, 'type')))

    def testToRepr(self):
        self.assertEqual('PublicationType<1:type>', repr(PublicationType(1, 'type')))

    def testRelations(self):
        i = PublicationType(1, 'type')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual([i], self.M.publication_types)
        self.assertEqual(self.M, i.citation)


class DatabaseTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = Database
        self.entity = namedtuple('Database', 'pmid name accession')
        self.defaults = self.entity(1, 'name', 'accession')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(name='other')
        self.assertDifference(accession='other')

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # NAME

    def testRequireName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptyName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # ACCESSION

    def testRequireAccession(self):
        self.assertNonNullValue(AssertionError, 'accession')

    def testRequireNonEmptyAccession(self):
        self.assertNonEmptyValue(AssertionError, 'accession')

    # METHODS

    def testToString(self):
        self.assertEqual('1\tname\taccession\n', str(Database(1, 'name', 'accession')))

    def testToRepr(self):
        self.assertEqual('Database<1:name:accession>', repr(Database(1, 'name', 'accession')))

    def testRelations(self):
        i = Database(1, 'name', 'accession')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual([i], self.M.databases)
        self.assertEqual(self.M, i.citation)


class ChemicalTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = Chemical
        self.entity = namedtuple('Chemical', 'pmid idx name')
        self.defaults = self.entity(1, 1, 'chem')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(name='other')
        self.assertDifference(idx=2)
        self.assertNotEqual(Chemical(1, 1, 'name', 'other'),
                            Chemical(1, 1, 'name', 'uid'))

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # IDX

    def testRequireIdx(self):
        self.assertNonNullValue(TypeError, 'idx')

    def testRequirePositiveIdx(self):
        self.assertPositiveValue(AssertionError, 'idx')

    # NAME

    def testRequireName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptyName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # UID

    def testRequireNonEmptyUid(self):
        self.assertBadExtraValue(AssertionError, 'uid', '')

    # METHODS

    def testToString(self):
        self.assertEqual('1\t1\tuid\tname\n', str(Chemical(1, 1, 'name', 'uid')))

    def testToRepr(self):
        self.assertEqual('Chemical<1:1>', repr(Chemical(1, 1, 'name')))

    def testRelations(self):
        i = Chemical(1, 1, 'name')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual([i], self.M.chemicals)
        self.assertEqual(self.M, i.citation)


class KeywordTest(TestCase, TestMixin):
    def setUp(self):
        InitDb(URI, module=dbapi2)
        self.sess = Session()
        self.M = DefaultCitation()
        self.sess.add(self.M)
        self.klass = Keyword
        self.entity = namedtuple('Keyword', 'pmid owner cnt name')
        self.defaults = self.entity(1, 'NASA', 1, 'keyword')

    def testCreate(self):
        self.assertCreate()

    def testEquals(self):
        self.assertSame()
        self.assertDifference(cnt=2)
        self.assertDifference(owner='NLM')
        self.assertDifference(name='other')
        self.assertNotEqual(Keyword(1, 'NASA', 1, 'name', True),
                            Keyword(1, 'NASA', 1, 'name', False))

    # PMID

    def testRequirePmid(self):
        self.assertNonNullValue(TypeError, 'pmid')

    # OWNER

    def testRequireOwner(self):
        self.assertNonNullValue(AssertionError, 'owner')

    def testRequireValidOwner(self):
        self.assertBadValue('WRONG', AssertionError, 'owner')

    # CNT

    def testRequireCnt(self):
        self.assertNonNullValue(TypeError, 'cnt')

    def testRequirePositiveCnt(self):
        self.assertPositiveValue(AssertionError, 'cnt')

    # NAME

    def testRequireName(self):
        self.assertNonNullValue(AssertionError, 'name')

    def testRequireNonEmptyName(self):
        self.assertNonEmptyValue(AssertionError, 'name')

    # MAJOR

    def testMajorIsBool(self):
        self.sess.add(Keyword(1, 'NLM', 1, 'name', None))
        self.assertRaises(IntegrityError, self.sess.commit)

    # METHODS

    def testToString(self):
        self.assertEqual('1\tNASA\t1\tF\tname\n', str(Keyword(1, 'NASA', 1, 'name')))
        self.assertEqual('1\tNASA\t1\tT\tname\n', str(Keyword(1, 'NASA', 1, 'name', True)))

    def testToRepr(self):
        self.assertEqual('Keyword<1:NASA:1>', repr(Keyword(1, 'NASA', 1, 'name')))

    def testRelations(self):
        i = Keyword(1, 'NASA', 1, 'name')
        self.sess.add(i)
        self.sess.commit()
        self.assertEqual([i], self.M.keywords)
        self.assertEqual(self.M, i.citation)


if __name__ == '__main__':
    main()
