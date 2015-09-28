"""
.. py:module:: medic.orm
   :synopsis: A SQLAlchemy-based MEDLINE DB ORM.

.. moduleauthor:: Florian Leitner <florian.leitner@gmail.com>
.. License: GNU Affero GPL v3 (http://www.gnu.org/licenses/agpl.html)
"""

import logging
from datetime import date
from sqlalchemy import engine, select, and_, Enum
from sqlalchemy import event
# from sqlalchemy.engine import RowProxy
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, session
from sqlalchemy.orm.collections import column_mapped_collection
from sqlalchemy.schema import \
    Column, CheckConstraint, ForeignKeyConstraint, ForeignKey, Index
from sqlalchemy.types import \
    Boolean, BigInteger, Date, SmallInteger, Unicode, UnicodeText

__all__ = [
    'Citation', 'Abstract', 'Author', 'Chemical', 'Database', 'Descriptor',
    'Identifier', 'Keyword', 'PublicationType', 'Qualifier', 'Section'
]

_Base = declarative_base()
_db = None
_session = lambda *args, **kwds: None

NULL = lambda s: '\\N' if s is None else s
DATE = lambda s: '\\N' if s is None else s.isoformat()
STRING = lambda s: s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')

logger = logging.getLogger(__name__)


def InitDb(*args, **kwds):
    """
    Create a new DBAPI connection pool.

    The most common and only required argument is the connection URL.
    The URL can either be a string or a `sqlalchemy.engine.url.URL`.
    This method has not return value and needs to be called only once per process.

    See `sqlalchemy.engine.create_engine`.
    """
    global _Base
    global _db
    global _session

    if len(args) > 0:
        # inject the foreign key pragma when using SQLite databases to ensure integrity
        # http://docs.sqlalchemy.org/en/rel_0_8/dialects/sqlite.html#foreign-key-support
        if (isinstance(args[0], str) and args[0].startswith('sqlite')) or \
                (isinstance(args[0], URL) and args[0].get_dialect() == 'sqlite'):
            # noinspection PyUnusedLocal
            @event.listens_for(engine.Engine, "connect")
            def set_sqlite_pragma(dbapi_connection, _):
                """Injection to enable foreign keys and make SQLite a bit faster."""
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA synchronous=OFF")
                cursor.execute("PRAGMA page_size=65536")
                cursor.close()

    _db = engine.create_engine(*args, **kwds)
    _Base.metadata.create_all(_db)
    _session = session.sessionmaker(bind=_db)
    logger.debug("DB bound to %s", _db)
    return None


def Session(*args, **kwds):
    """
    Create a new DBAPI session object to work with.

    See `sqlalchemy.orm.session.Session`.
    """
    return _session(*args, **kwds)


def _fetch_first(query):
    """Given a *query*, fetch the first row and return the first element or ``None``."""
    conn = _db.engine.connect()
    logger.debug("%s", query)

    try:
        result = conn.execute(query)
        row = result.first()
        return row[0] if row else None
    finally:
        conn.close()


def _fetch_all(query):
    """Given a *query*, fetch and return all rows."""
    conn = _db.engine.connect()
    logger.debug("%s", query)

    try:
        result = conn.execute(query)
        return result.fetchall()
    finally:
        conn.close()


# noinspection PyUnresolvedReferences
class SelectMixin(object):
    """
    Mixin for child tables to select rows (and columns)
    for a particular parent object
    (i.e., `Citation`, except for `Qualifier`).
    """

    @classmethod
    def _buildQuery(cls, columns, parent_pk):
        if isinstance(parent_pk, tuple):
            last = len(cls.__table__.primary_key) - 1
            assert len(parent_pk) == last, \
                "wrong parent_pk %s (%d)" % (str(parent_pk), len(cls.__table__.primary_key))
            where_clause = [
                col == parent_pk[idx] for idx, col in
                enumerate(cls.__table__.primary_key) if idx != last
            ]
            return select(columns, and_(*where_clause))
        else:
            assert isinstance(parent_pk, int), \
                "parent_pk not a PMID: %s" % str(parent_pk)
            return select(columns, cls.__table__.c.pmid == parent_pk)

    @classmethod
    def select(cls, parent_pk, attributes):
        """
        Return *attributes* columns for all rows that match the object's
        *parent_pk* (the pmid in most cases, but (pmid, num) for
        `Qualifier`).

        :arg  parent_pk: The parent objects pk
        :type parent_pk: :class:`tuple` or :keyword:`scalar`

        :arg  attributes: A list of attribute (column) names
        :type attributes: :class:`list` of :class:`str`
        """
        mapping = dict((col.key, col) for col in cls.__table__.c)
        columns = [mapping[name] for name in attributes]
        query = cls._buildQuery(columns, parent_pk)
        return _fetch_all(query)

    @classmethod
    def selectAll(cls, parent_pk):
        """
        Return all columns for all rows that match the *parent_pk*.

        See :meth:`select` for details.
        """
        query = cls._buildQuery([cls.__table__], parent_pk)
        return _fetch_all(query)


class Identifier(_Base, SelectMixin):
    """
    All known unique IDs for a PubMed record.

    Attributes:

        pmid
            the record this alternate ID belongs to
        namespace
            the type of ID (doi, pii, pmc, pubmed, etc.)
        value
            the actual ID string

    Primary Key: ``(pmid, namespace)``
    """

    __tablename__ = 'identifiers'

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    namespace = Column(
        Unicode(length=32), CheckConstraint("namespace <> ''"), primary_key=True
    )
    value = Column(
        Unicode(length=256), CheckConstraint("value <> ''"), nullable=False
    )

    def __init__(self, pmid: int, namespace: str, value: str):
        assert pmid > 0, pmid
        assert namespace
        assert value
        self.pmid = pmid
        self.namespace = namespace
        self.value = value

    def __str__(self):
        return '{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.namespace), STRING(self.value)
        )

    def __repr__(self):
        return "Identifier<{}:{}>".format(self.pmid, self.namespace)

    def __eq__(self, other):
        return isinstance(other, Identifier) and \
            self.pmid == other.pmid and \
            self.namespace == other.namespace and \
            self.value == other.value

    @classmethod
    def pmid2doi(cls, pmid: int):
        """Convert a PMID to a DOI (or ``None`` if no mapping is found)."""
        c = cls.__table__.c
        query = select(
            [c.value], (c.namespace == 'doi') & (c.pmid == pmid)
        )
        return _fetch_first(query)

    @classmethod
    def doi2pmid(cls, doi: str):
        """Convert a DOI to a PMID (or ``None`` if no mapping is found)."""
        c = cls.__table__.c
        query = select(
            [c.pmid], (c.namespace == 'doi') & (c.value == doi)
        )
        return _fetch_first(query)

    @classmethod
    def mapDois2Pmids(cls, dois: list):
        """
        Return a mapping :class:`dict` for a list of DOIs to their PMIDs
        (or and empty :class:`dict` if no mapping is found).

        If for a given DOI no mapping exists, it is no included in the
        returned dictionary.
        """
        if not len(dois):
            return {}

        c = cls.__table__.c
        query = select(
            [c.value, c.pmid], (c.namespace == 'doi') & c.value.in_(dois)
        )
        mappings = _fetch_all(query)
        return dict(mappings) if mappings is not None else {}

    @classmethod
    def mapPmids2Dois(cls, pmids: list):
        """
        Return a mapping :class:`dict` for a list of PMIDs to their DOIs
        (or and empty :class:`dict` if no mapping is found).

        If for a given PMID no mapping exists, it is no included in the
        returned dictionary.
        """
        if not len(pmids):
            return {}

        t = cls.__table__
        query = select(
            [t.c.pmid, t.c.value], (t.c.namespace == 'doi') & t.c.pmid.in_(pmids)
        )
        mappings = _fetch_all(query)
        return dict(mappings) if mappings is not None else {}


# Index to make queries for a particular ID, e.g., a DOI, faster.
Index(
    'identifiers_namespace_value_idx',
    Identifier.__table__.c.namespace, Identifier.__table__.c.value
)


class Author(_Base, SelectMixin):
    """
    Author names for a PubMed record.

    Attributes:

        pmid
            the record this author belongs to
        pos
            the order/position of the name in the PubMed record (starting from 1)
        name
            an author's last name or the collective's name (never empty)
        initials
            an author's initials *
        forename
            the expansion of the initials known to PubMed *
        suffix
            an author name's suffix *

    * empty string if explicitly non-existent, NULL if unknown

    Primary Key: ``(pmid, pos)``
    """

    __tablename__ = 'authors'

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    pos = Column(SmallInteger, CheckConstraint("pos > 0"), primary_key=True)
    name = Column(
        UnicodeText, CheckConstraint("name <> ''"), nullable=False
    )
    initials = Column(Unicode(length=128), nullable=True)
    forename = Column(Unicode(length=256), nullable=True)
    suffix = Column(Unicode(length=128), nullable=True)

    def __init__(self, pmid: int, pos: int, name: str,
                 initials: str=None, forename: str=None, suffix: str=None):
        assert pmid > 0, pmid
        assert pos > 0, pos
        assert name, repr(name)
        self.pmid = pmid
        self.pos = pos
        self.name = name
        self.initials = initials
        self.forename = forename
        self.suffix = suffix

    def __str__(self):
        return "{}\t{}\t{}\t{}\t{}\t{}\n".format(
            NULL(self.pmid), NULL(self.pos), STRING(self.name),
            NULL(self.initials), NULL(self.forename), NULL(self.suffix))

    def __repr__(self):
        return "Author<{}:{}>".format(self.pmid, self.pos)

    def __eq__(self, other):
        return isinstance(other, Author) and \
            self.pmid == other.pmid and \
            self.pos == other.pos and \
            self.name == other.name and \
            self.initials == other.initials and \
            self.forename == other.forename and \
            self.suffix == other.suffix

    def fullName(self):
        """
        Return the full name of this author
        (using forename or initials, last, and suffix).
        """
        name = []
        if self.forename:
            name.append(self.forename)
        elif self.initials:
            name.append(self.initials)
        name.append(self.name)
        if self.suffix:
            name.append(self.suffix)
        return ' '.join(name)

    def shortName(self):
        """Return the short name of this author (using initials and last)."""
        name = []
        if self.initials:
            name.append(self.initials)
        elif self.forename:
            name.append(''.join([n[0] for n in self.forename.split()]))
        name.append(self.name)
        return ' '.join(name)


class Qualifier(_Base, SelectMixin):
    """
    One of a MeSH descriptor's qualifiers for a record.

    Attributes:

        pmid
            the record this qualifier name belongs to
        num
            the descriptor order in the record (starting from 1)
        sub
            the qualifier order within the descriptor (starting from 1)
        major
            ``True`` if major, ``False`` if minor
        name
            the qualifier (name)

    Primary Key: ``(pmid, num, sub)``
    """

    __tablename__ = 'qualifiers'
    __table_args__ = (
        ForeignKeyConstraint(
            ('pmid', 'num'), ('descriptors.pmid', 'descriptors.num'),
            ondelete="CASCADE"
        ),
    )

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    num = Column(SmallInteger, primary_key=True)
    sub = Column(SmallInteger, CheckConstraint("sub > 0"), primary_key=True)
    major = Column(Boolean, nullable=False)
    name = Column(UnicodeText, CheckConstraint("name <> ''"), nullable=False)

    def __init__(self, pmid: int, num: int, sub: int, name: str, major: bool=False):
        assert pmid > 0, pmid
        assert num > 0, num
        assert sub > 0, sub
        assert name, repr(name)
        self.pmid = pmid
        self.num = num
        self.sub = sub
        self.major = major
        self.name = name

    def __str__(self):
        return '{}\t{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.num), NULL(self.sub),
            'T' if self.major else 'F', STRING(self.name),
        )

    def __repr__(self):
        return "Qualifier<{}:{}:{}>".format(self.pmid, self.num, self.sub)

    def __eq__(self, other):
        return isinstance(other, Qualifier) and \
            self.pmid == other.pmid and \
            self.num == other.num and \
            self.sub == other.sub and \
            self.name == other.name and \
            self.major == other.major


class Descriptor(_Base, SelectMixin):
    """
    A MeSH descriptor for the record.

    Attributes:

        pmid
            the record this descriptor name belongs to
        num
            the descriptor order in the record (starting from 1)
        major
            ``True`` if major, ``False`` if minor
        name
            the descriptor

    Relations:

        qualifiers
            a :class:`list` of the descriptor's qualifiers

    Primary Key: ``(pmid, num)``
    """

    __tablename__ = 'descriptors'

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    num = Column(SmallInteger, CheckConstraint("num > 0"), primary_key=True)
    major = Column(Boolean, nullable=False)
    name = Column(UnicodeText, CheckConstraint("name <> ''"), nullable=False)

    qualifiers = relation(
        Qualifier, backref='descriptor', cascade="all",
        order_by=Qualifier.__table__.c.sub
    )

    def __init__(self, pmid: int, num: int, name: str, major: bool=False):
        assert pmid > 0, pmid
        assert num > 0, num
        assert name, repr(name)
        self.pmid = pmid
        self.num = num
        self.major = major
        self.name = name

    def __str__(self):
        return '{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.num), 'T' if self.major else 'F', STRING(self.name)
        )

    def __repr__(self):
        return "Descriptor<{}:{}>".format(self.pmid, self.num)

    def __eq__(self, other):
        return isinstance(other, Descriptor) and \
            self.pmid == other.pmid and \
            self.num == other.num and \
            self.name == other.name and \
            self.major == other.major


class Chemical(_Base, SelectMixin):
    """
    References to chemicals and substances curated my the NLM.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        idx
            the order in the XML record
        uid
            the chemical's "unique identifier" (registry number, etc.)
        name
            the chemical's name

    Primary Key: ``(pmid, idx)``
    """

    __tablename__ = 'chemicals'

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    idx = Column(SmallInteger, CheckConstraint("idx > 0"), primary_key=True)
    uid = Column(Unicode(length=256), CheckConstraint("uid <> ''"), nullable=True)
    name = Column(Unicode(length=256), CheckConstraint("name <> ''"), nullable=False)

    def __init__(self, pmid: int, idx: int, name: str, uid: str=None):
        assert pmid > 0, pmid
        assert idx > 0, idx
        assert name, repr(name)
        assert uid is None or uid, repr(uid)
        self.pmid = pmid
        self.idx = idx
        self.uid = uid
        self.name = name

    def __str__(self):
        return '{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.idx), NULL(self.uid), STRING(self.name)
        )

    def __repr__(self):
        return "Chemical<{}:{}>".format(self.pmid, self.idx)

    def __eq__(self, other):
        return isinstance(other, Chemical) and \
            self.pmid == other.pmid and \
            self.idx == other.idx and \
            self.uid == other.uid and \
            self.name == other.name


class PublicationType(_Base, SelectMixin):
    """
    Records the type of publication this citation refers to.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        value
            the publication type value

    Primary Key: ``(pmid, value)``
    """

    __tablename__ = 'publication_types'

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    value = Column(Unicode(length=256), CheckConstraint("value <> ''"), primary_key=True)

    def __init__(self, pmid: int, value: str):
        assert pmid > 0, pmid
        assert value, repr(value)
        self.pmid = pmid
        self.value = value

    def __str__(self):
        return '{}\t{}\n'.format(NULL(self.pmid), NULL(self.value))

    def __repr__(self):
        return "PublicationType<{}:{}>".format(self.pmid, self.value)

    def __eq__(self, other):
        return isinstance(other, PublicationType) and \
            self.pmid == other.pmid and \
            self.value == other.value


class Database(_Base, SelectMixin):
    """
    References to external databases curated by the NLM.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        name
            the referenced database' name
        accession
            the referenced record's identifier

    Primary Key: ``(pmid, name, accession)``
    """

    __tablename__ = 'databases'

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    name = Column(Unicode(length=256), CheckConstraint("name <> ''"), primary_key=True)
    accession = Column(Unicode(length=256), CheckConstraint("accession <> ''"), primary_key=True)

    def __init__(self, pmid: int, name: str, accession: str):
        assert pmid > 0, pmid
        assert name, repr(name)
        assert accession, repr(accession)
        self.pmid = pmid
        self.name = name
        self.accession = accession

    def __str__(self):
        return '{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.name), STRING(self.accession)
        )

    def __repr__(self):
        return "Database<{}:{}:{}>".format(self.pmid, self.name, self.accession)

    def __eq__(self, other):
        return isinstance(other, Database) and \
            self.pmid == other.pmid and \
            self.name == other.name and \
            self.accession == other.accession


class Keyword(_Base, SelectMixin):
    """
    Keywords, external or curated by the NLM.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        owner
            the entity that provided the keyword
        cnt
            a unique counter for all keywords from a given owner and record
            (starting from 1)
        major
            if the keyword is a major topic of this article
        name
            the keyword itself

    Primary Key: ``(pmid, owner, cnt)``
    """

    __tablename__ = 'keywords'

    OWNERS = frozenset({'NASA', 'PIP', 'KIE', 'NLM', 'NOTNLM', 'HHS'})

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    owner = Column(Enum(*OWNERS, name='owner'), primary_key=True)
    cnt = Column(SmallInteger, CheckConstraint("cnt > 0"), primary_key=True)
    major = Column(Boolean, nullable=False)
    name = Column(UnicodeText, CheckConstraint("name <> ''"), nullable=False)

    def __init__(self, pmid: int, owner: str, cnt: int, name: str, major: bool=False):
        assert pmid > 0, pmid
        assert owner in Keyword.OWNERS, repr(owner)
        assert cnt > 0, cnt
        assert name, repr(name)
        self.pmid = pmid
        self.owner = owner
        self.cnt = cnt
        self.major = major
        self.name = name

    def __str__(self):
        return '{}\t{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.owner), NULL(self.cnt),
            'T' if self.major else 'F', STRING(self.name)
        )

    def __repr__(self):
        return "Keyword<{}:{}:{}>".format(
            self.pmid, self.owner, self.cnt
        )

    def __eq__(self, other):
        return isinstance(other, Keyword) and \
            self.pmid == other.pmid and \
            self.owner == other.owner and \
            self.cnt == other.cnt and \
            self.major == other.major and \
            self.name == other.name


class Section(_Base, SelectMixin):
    """
    The text sections of the records.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        source
            the abstract's source (see `Abstract.SOURCES`)
        seq
            the sequence of sections in the abstract (starting from 1)
        name
            the name of the section (Abstract, Background, Methods, Unassigned, ...)
        label
            section label as defined by the publisher (if any)
        content
            the text content of the section
        truncated
            if the text content was truncated by PubMed
            that means the content contained the string "(ABSTRACT TRUNCATED AT 250 WORDS)" at
            its end; this message is removed by the parser

    Primary Key: ``(pmid, source, seq)``
    """
    __tablename__ = 'sections'
    __table_args__ = (
        ForeignKeyConstraint(
            ('pmid', 'source'), ('abstracts.pmid', 'abstracts.source'),
            ondelete="CASCADE"
        ),
    )

    SOURCES = frozenset({'NLM', 'AAMC', 'AIDS', 'KIE', 'PIP', 'NASA', 'Publisher'})

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    source = Column(Enum(*SOURCES, name='source'), primary_key=True)
    seq = Column(SmallInteger, CheckConstraint("seq > 0"), primary_key=True)
    name = Column(Unicode(length=64), CheckConstraint("name <> ''"), nullable=False)
    label = Column(Unicode(length=256), CheckConstraint("label <> ''"), nullable=True)
    content = Column(UnicodeText, CheckConstraint("content <> ''"), nullable=False)
    truncated = Column(Boolean, nullable=False, default=False)

    def __init__(self, pmid: int, source: str, seq: int, name: str, content: str,
                 label: str=None, truncated: bool=False):
        assert pmid > 0, pmid
        assert source in Section.SOURCES, repr(source)
        assert seq > 0, seq
        assert name, repr(name)
        assert content, repr(content)
        assert label is None or label, repr(label)
        self.pmid = pmid
        self.source = source
        self.seq = seq
        self.name = name
        self.label = label
        self.content = content
        self.truncated = bool(truncated)

    def __str__(self):
        return '{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.source), NULL(self.seq), NULL(self.name),
            NULL(self.label), STRING(self.content), 'T' if self.truncated else 'F'
        )

    def __repr__(self):
        return "Section<{}:{}:{}>".format(self.pmid, self.source, self.seq)

    def __eq__(self, other):
        return isinstance(other, Section) and \
            self.pmid == other.pmid and \
            self.source == other.source and \
            self.seq == other.seq and \
            self.name == other.name and \
            self.label == other.label and \
            self.content == other.content and \
            self.truncated == other.truncated


class Abstract(_Base, SelectMixin):
    """
    The abstract of the records.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        source
            the abstract's source (see `Abstract.SOURCES`)
        copyright
            the abstract's copyright notice (if any)

    Primary Key: ``(pmid, source)``
    """
    __tablename__ = 'abstracts'

    SOURCES = Section.SOURCES

    pmid = Column(BigInteger, ForeignKey('citations.pmid', ondelete="CASCADE"), primary_key=True)
    source = Column(Enum(*SOURCES, name='source'), primary_key=True)
    copyright = Column(UnicodeText, CheckConstraint("copyright <> ''"), nullable=True)

    sections = relation(
        Section, backref='abstract', cascade="all",
        order_by=Section.__table__.c.seq
    )

    def __init__(self, pmid: int, source: str='NLM', copy: str=None):
        assert pmid > 0, pmid
        assert source in Abstract.SOURCES
        assert copy is None or copy, repr(copy)
        self.pmid = pmid
        self.source = source
        self.copyright = copy

    def __str__(self):
        return '{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.source), NULL(self.copyright)
        )

    def __repr__(self):
        return "Abstract<{}:{}>".format(self.pmid, self.source)

    def __eq__(self, other):
        return isinstance(other, Abstract) and \
            self.pmid == other.pmid and \
            self.source == other.source and \
            self.copyright == other.copyright


class Citation(_Base):
    """
    A MEDLINE or PubMed citation record.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        status
            the current status of this record (see `Citation.STATES`)
        year
            the year of publication
        title
            the record's title
        journal
            the journal name (Medline TA)
        pub_date
            the string of the publication date
        issue
            the journal issue string
        pagination
            the pagination string of the journal
        created
            the record's creation date
        completed
            the record's completion date
        revised
            the record's revision date
        modified
            the date the record was last modified in the DB

    Relations:

        sections
            a :class:`list` of the record's text sections
        authors
            a :class:`list` of the record's author names
        identifiers
            a :class:`dict` of the record's alternate IDs using the
            :attr:`AlternateID.key` a dictionary keys
        descriptors
            a :class:`list` of the record's MeSH descriptors
        qualifiers
            a :class:`list` of the record's MeSH qualifiers
        chemicals
            a :class:`list` of the record's chemicals
        databases
            a :class:`list` of the record's external DB references

    Primary Key: ``pmid``
    """

    STATES = frozenset({'Completed', 'In-Process', 'PubMed-not-MEDLINE',
                        'In-Data-Review', 'Publisher', 'MEDLINE', 'OLDMEDLINE'})
    CHILDREN = (
        Section, Identifier, Database, Author, Descriptor, Qualifier,  # Qualifier last!
    )
    TABLENAMES = [cls.__tablename__ for cls in CHILDREN]
    TABLES = {cls.__tablename__: cls.__table__ for cls in CHILDREN}

    __tablename__ = 'citations'

    abstracts = relation(
        Abstract, backref='citation', cascade='all, delete-orphan',
        collection_class=column_mapped_collection(Abstract.source)
    )
    authors = relation(
        Author, backref='citation', cascade='all, delete-orphan',
        order_by=Author.__table__.c.pos
    )
    chemicals = relation(Chemical, backref='citation', cascade='all, delete-orphan')
    databases = relation(Database, backref='citation', cascade='all, delete-orphan')
    descriptors = relation(
        Descriptor, backref='citation', cascade='all, delete-orphan',
        order_by=Descriptor.__table__.c.num
    )
    identifiers = relation(
        Identifier, backref='citation', cascade='all, delete-orphan',
        collection_class=column_mapped_collection(Identifier.namespace)
    )
    keywords = relation(
        Keyword, backref='citation', cascade='all, delete-orphan',
        order_by=Keyword.__table__.c.owner
    )
    publication_types = relation(PublicationType, backref='citation', cascade='all, delete-orphan')
    qualifiers = relation(Qualifier, backref='citation')
    sections = relation(Section, backref='citation')

    pmid = Column(BigInteger, CheckConstraint('pmid > 0'), primary_key=True, autoincrement=False)
    status = Column(Enum(*STATES, name='state'), nullable=False)
    year = Column(SmallInteger, CheckConstraint('year > 1000 AND year < 3000'), nullable=False)
    title = Column(UnicodeText, CheckConstraint("title <> ''"), nullable=False)
    journal = Column(Unicode(length=256), CheckConstraint("journal <> ''"), nullable=False)
    pub_date = Column(Unicode(length=256), CheckConstraint("pub_date <> ''"), nullable=False)
    issue = Column(Unicode(length=256), CheckConstraint("issue <> ''"), nullable=True)
    pagination = Column(Unicode(length=256), CheckConstraint("pagination <> ''"), nullable=True)
    created = Column(Date, nullable=False)
    completed = Column(Date, nullable=True)
    revised = Column(Date, nullable=True)
    modified = Column(Date, default=date.today, onupdate=date.today, nullable=False)

    def __init__(self,
                 pmid: int, status: str, title: str, journal: str, pub_date: str, created: date,
                 completed: date=None, revised: date=None, issue: str=None, pagination: str=None):
        assert pmid > 0, pmid
        assert status in Citation.STATES, repr(status)
        assert title, repr(title)
        assert journal, repr(journal)
        assert pub_date, repr(pub_date)
        assert isinstance(created, date), repr(created)
        assert completed is None or isinstance(completed, date), repr(completed)
        assert revised is None or isinstance(revised, date), repr(revised)
        assert pagination is None or pagination
        assert issue is None or issue
        assert len(pub_date) >= 4, pub_date
        self.pmid = pmid
        self.status = status
        self.title = title
        self.journal = journal
        self.pub_date = pub_date
        self.year = int(pub_date[:4])
        self.issue = issue
        self.pagination = pagination
        self.created = created
        self.completed = completed
        self.revised = revised

    def __str__(self):
        return '{}\n'.format('\t'.join(map(str, [
            NULL(self.pmid), NULL(self.status), NULL(self.year), STRING(self.title),
            STRING(self.journal), STRING(self.pub_date), NULL(self.issue), NULL(self.pagination),
            DATE(self.created), DATE(self.completed), DATE(self.revised),
            DATE(date.today() if self.modified is None else self.modified)
        ])))

    def __repr__(self):
        return "Citation<{}>".format(self.pmid)

    def __eq__(self, other):
        return isinstance(other, Citation) and \
            self.pmid == other.pmid and \
            self.status == other.status and \
            self.year == other.year and \
            self.journal == other.journal and \
            self.title == other.title and \
            self.pub_date == other.pub_date and \
            self.issue == other.issue and \
            self.pagination == other.pagination and \
            self.created == other.created and \
            self.completed == other.completed and \
            self.revised == other.revised

    def citation(self):
        issue = '; {}'.format(self.issue) if self.issue else ""
        pagination = ': {}'.format(self.pagination) if self.pagination else ""
        return "{}{}{}".format(self.pub_date, issue, pagination)

    @classmethod
    def insert(cls, data: dict):
        """
        Insert *data* into all relevant tables.
        """
        target_ins = dict(
            (tname, cls.TABLES[tname].insert())
            for tname in cls.TABLENAMES
        )
        conn = _db.engine.connect()
        transaction = conn.begin()

        try:
            if cls.__tablename__ in data and len(data[cls.__tablename__]):
                conn.execute(
                    cls.__table__.insert(), data[cls.__tablename__]
                )

            if Abstract.__tablename__ in data and len(data[Abstract.__tablename__]):
                conn.execute(
                    Abstract.__table__.insert(), data[Abstract.__tablename__]
                )

            if Descriptor.__tablename__ in data and len(data[Descriptor.__tablename__]):
                conn.execute(
                    Descriptor.__table__.insert(), data[Descriptor.__tablename__]
                )

            for tname in cls.TABLENAMES:
                if tname == Descriptor.__tablename__ or tname == Abstract.__tablename__:
                    pass
                elif tname in data and len(data[tname]):
                    conn.execute(target_ins[tname], data[tname])

            transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            conn.close()

    @classmethod
    def select(cls, pmids: list, attributes: iter):
        """
        Return the `pmid` and *attributes*
        for each row as a `sqlalchemy.engine.RowProxy`
        that matches one of the *pmids*.
        """
        if not len(pmids):
            return []

        c = cls.__table__.c
        mapping = {col.key: col for col in c}
        columns = [mapping[name] for name in attributes]
        columns.insert(0, c.pmid)
        query = select(columns, c.pmid.in_(pmids))
        return _fetch_all(query)

    @classmethod
    def selectAll(cls, pmids: list):
        """
        Return all columns
        for each row as a `sqlalchemy.engine.RowProxy`
        that matches one of the *pmids*.
        """
        if not len(pmids):
            return []

        c = cls.__table__.c
        query = select([cls.__table__], c.pmid.in_(pmids))
        return _fetch_all(query)

    @classmethod
    def delete(cls, primary_keys: list):
        """
        Delete records and their dependent entities (authors, identifiers,
        etc.) for the given *primary_keys* (a list of PMIDs).
        """
        if not len(primary_keys):
            return

        t = cls.__table__
        query = t.delete(t.c.pmid.in_(primary_keys))
        conn = _db.engine.connect()
        transaction = conn.begin()

        try:
            conn.execute(query)
            transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            conn.close()

    @classmethod
    def existing(cls, pmids: list):
        """Return the sub- `set` of all *pmids* that exist in the DB."""
        if not len(pmids):
            return set()

        c = cls.__table__.c
        query = select([c.pmid], c.pmid.in_(pmids))
        conn = _db.engine.connect()

        try:
            return {row[0] for row in conn.execute(query)}
        finally:
            conn.close()

    @classmethod
    def missing(cls, pmids: list):
        """Return the sub- `set` of all *pmids* that do not exist in the DB."""
        return set(pmids) - Citation.existing(pmids)

    @classmethod
    def modifiedBefore(cls, pmids: list, before: date):
        """
        Return the sub- `set` of all *pmids* that have been `modified`
        *before* a `datetime.date` in the DB.
        """
        if not len(pmids):
            return set()

        c = cls.__table__.c
        query = select(
            [c.pmid], c.pmid.in_(pmids) & (c.modified < before)
        )
        conn = _db.engine.connect()

        try:
            return set(row[0] for row in conn.execute(query))
        finally:
            conn.close()
