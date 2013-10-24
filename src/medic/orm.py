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
from sqlalchemy.engine import RowProxy
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, session
from sqlalchemy.orm.collections import column_mapped_collection
from sqlalchemy.schema import \
    Column, CheckConstraint, ForeignKeyConstraint, ForeignKey, Index
from sqlalchemy.types import \
    Boolean, BigInteger, Date, SmallInteger, Unicode, UnicodeText

__all__ = [
    'Medline', 'Author', 'Chemical', 'Database', 'Descriptor', 'Identifier', 'Qualifier', 'Section'
]

_Base = declarative_base()
_db = None
_session = lambda *args, **kwds: None

NULL = lambda s: '\\N' if s is None else s
DATE = lambda s: '\\N' if s is None else s.isoformat()

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
            #noinspection PyUnusedLocal
            @event.listens_for(engine.Engine, "connect")
            def set_sqlite_pragma(dbapi_connection, _):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
    _db = engine.create_engine(*args, **kwds)
    _Base.metadata.create_all(_db)
    _session = session.sessionmaker(bind=_db)
    logger.debug("DB bound to %s", _db)
    return None


def Session(*args, **kwds) -> session.Session:
    """
    Create a new DBAPI session object to work with.

    See `sqlalchemy.orm.session.Session`.
    """
    return _session(*args, **kwds)


def _fetch_first(query) -> RowProxy:
    "Given a *query*, fetch the first row and return the first element or ``None``."
    conn = _db.engine.connect()
    logger.debug("%s", query)

    try:
        result = conn.execute(query)
        row = result.first()
        return row[0] if row else None
    finally:
        conn.close()


def _fetch_all(query) -> iter([RowProxy]):
    "Given a *query*, fetch and return all rows."
    conn = _db.engine.connect()
    logger.debug("%s", query)

    try:
        result = conn.execute(query)
        return result.fetchall()
    finally:
        conn.close()


class SelectMixin(object):
    """
    Mixin for child tables to select rows (and columns)
    for a particular parent object
    (i.e., `Medline`, except for `Qualifier`).
    """

    # noinspection PyUnresolvedReferences
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
    def select(cls, parent_pk, attributes) -> iter([RowProxy]):
        """
        Return *attributes* columns for all rows that match the object's
        *parent_pk* (the pmid in most cases, but (pmid, num) for
        `Qualifier`).

        :arg  parent_pk: The parent objects pk
        :type parent_pk: :class:`tuple` or :keyword:`scalar`

        :arg  attributes: A list of attribute (column) names
        :type attributes: :class:`list` of :class:`str`
        """
        # noinspection PyUnresolvedReferences
        mapping = dict((col.key, col) for col in cls.__table__.c)
        columns = [mapping[name] for name in attributes]
        query = cls._buildQuery(columns, parent_pk)
        return _fetch_all(query)

    @classmethod
    def selectAll(cls, parent_pk) -> iter([RowProxy]):
        """
        Return all columns for all rows that match the *parent_pk*.

        See :meth:`select` for details.
        """
        # noinspection PyUnresolvedReferences
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

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
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
            NULL(self.pmid), NULL(self.namespace), NULL(self.value)
        )

    def __repr__(self):
        return "Identifier<{}:{}>".format(self.pmid, self.namespace)

    def __eq__(self, other):
        return isinstance(other, Identifier) and \
               self.pmid == other.pmid and \
               self.namespace == other.namespace and \
               self.value == other.value

    @classmethod
    def pmid2doi(cls, pmid: int) -> str:
        "Convert a PMID to a DOI (or ``None`` if no mapping is found)."
        c = cls.__table__.c
        query = select(
            [c.value], (c.namespace == 'doi') & (c.pmid == pmid)
        )
        return _fetch_first(query)

    @classmethod
    def doi2pmid(cls, doi: str) -> int:
        "Convert a DOI to a PMID (or ``None`` if no mapping is found)."
        c = cls.__table__.c
        query = select(
            [c.pmid], (c.namespace == 'doi') & (c.value == doi)
        )
        return _fetch_first(query)

    @classmethod
    def mapDois2Pmids(cls, dois: list) -> dict:
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
    def mapPmids2Dois(cls, pmids: list) -> dict:
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

    * empty string if explicitly non-existant, NULL if unknown

    Primary Key: ``(pmid, pos)``
    """

    __tablename__ = 'authors'

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
    pos = Column(SmallInteger, CheckConstraint("pos > 0"), primary_key=True)
    name = Column(
        UnicodeText, CheckConstraint("name <> ''"), nullable=False
    )
    initials = Column(Unicode(length=128), nullable=True)
    forename = Column(Unicode(length=128), nullable=True)
    suffix = Column(Unicode(length=128), nullable=True)

    def __init__(self, pmid: int, pos: int, name: str,
                 initials: str=None, forename: str=None, suffix: str=None):
        assert pmid > 0, pmid
        assert pos > 0, pos
        assert name
        self.pmid = pmid
        self.pos = pos
        self.name = name
        self.initials = initials
        self.forename = forename
        self.suffix = suffix

    def __str__(self):
        return "{}\t{}\t{}\t{}\t{}\t{}\n".format(
            NULL(self.pmid), NULL(self.pos), NULL(self.name),
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

    def fullName(self) -> str:
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

    def shortName(self) -> str:
        "Return the short name of this author (using initials and last)."
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
        name
            the qualifier (name)
        major
            ``True`` if major, ``False`` if minor

    Primary Key: ``(pmid, num, sub)``
    """

    __tablename__ = 'qualifiers'
    __table_args__ = (
        ForeignKeyConstraint(
            ('pmid', 'num'), ('descriptors.pmid', 'descriptors.num'),
            ondelete="CASCADE"
        ),
    )

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
    num = Column(SmallInteger, primary_key=True)
    sub = Column(SmallInteger, CheckConstraint("sub > 0"), primary_key=True)
    name = Column(
        UnicodeText, CheckConstraint("name <> ''"), nullable=False
    )
    major = Column(Boolean, nullable=False)

    def __init__(self, pmid: int, num: int, sub: int, name: str, major: bool=False):
        assert pmid > 0, pmid
        assert num > 0, num
        assert sub > 0, sub
        assert name
        self.pmid = pmid
        self.num = num
        self.sub = sub
        self.name = name
        self.major = major

    def __str__(self):
        return '{}\t{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.num), NULL(self.sub), NULL(self.name),
            'T' if self.major else 'F'
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
        name
            the descriptor
        major
            ``True`` if major, ``False`` if minor

    Relations:

        qualifiers
            a :class:`list` of the descriptor's qualifiers

    Primary Key: ``(pmid, num)``
    """

    __tablename__ = 'descriptors'

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
    num = Column(SmallInteger, CheckConstraint("num > 0"), primary_key=True)
    name = Column(
        UnicodeText, CheckConstraint("name <> ''"), nullable=False
    )
    major = Column(Boolean, nullable=False)

    qualifiers = relation(
        Qualifier, backref='descriptor', cascade="all",
        order_by=Qualifier.__table__.c.sub
    )

    def __init__(self, pmid: int, num: int, name: str, major: bool=False):
        assert pmid > 0
        assert num > 0
        assert name
        self.pmid = pmid
        self.num = num
        self.name = name
        self.major = major

    def __str__(self):
        return '{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.num), NULL(self.name),
            'T' if self.major else 'F'
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
        num
            the order in the XML record
        uid
            the chemical's "unique identifier" (registry number, etc.)
        name
            the chemical's name

    Primary Key: ``(pmid, num)``
    """

    __tablename__ = 'chemicals'

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
    num = Column(SmallInteger, CheckConstraint("num > 0"), primary_key=True)
    uid = Column(Unicode(length=256))
    name = Column(Unicode(length=256), CheckConstraint("name <> ''"), nullable=False)

    def __init__(self, pmid: int, num: int, uid, name: str):
        assert pmid > 0
        assert num > 0
        assert name
        self.pmid = pmid
        self.num = num
        self.uid = uid
        self.name = name

    def __str__(self):
        return '{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.num), NULL(self.uid), self.name
        )

    def __repr__(self):
        return "Chemical<{}:{}>".format(self.pmid, self.num)

    def __eq__(self, other):
        return isinstance(other, Chemical) and \
               self.pmid == other.pmid and \
               self.num == other.num


class Database(_Base, SelectMixin):
    """
    References to external databases curated my the NLM.

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

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
    name = Column(Unicode(length=256), CheckConstraint("name <> ''"), primary_key=True)
    accession = Column(Unicode(length=256), CheckConstraint("accession <> ''"), primary_key=True)

    def __init__(self, pmid: int, name: str, accession: str):
        assert pmid > 0
        assert name
        assert accession, repr(accession)
        self.pmid = pmid
        self.name = name
        self.accession = accession

    def __str__(self):
        return '{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.name), NULL(self.accession)
        )

    def __repr__(self):
        return "Database<{}:{}:{}>".format(self.pmid, self.name, self.accession)

    def __eq__(self, other):
        return isinstance(other, Database) and \
               self.pmid == other.pmid and \
               self.name == other.name and \
               self.accession == other.accession


class Section(_Base, SelectMixin):
    """
    The text sections of the records.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        seq
            the sequence of sections in the record (starting from 1)
        name
            the name of the section (see `Section.SECTIONS`)
        label
            section label as defined by the publisher (if any)
        content
            the text content of this section

    Primary Key: ``(pmid, num)``
    """

    SECTIONS = frozenset({'Title', 'Abstract', 'Vernacular', 'Copyright',
                          'Background', 'Objective', 'Methods', 'Results', 'Conclusions',
                          'Unassigned', 'Unlabelled'})

    __tablename__ = 'sections'

    pmid = Column(BigInteger, ForeignKey('records', ondelete="CASCADE"), primary_key=True)
    seq = Column(SmallInteger, CheckConstraint("seq > 0"), primary_key=True)
    name = Column(Enum(*SECTIONS, name='section'), nullable=False)
    label = Column(Unicode(length=256))
    content = Column(
        UnicodeText, CheckConstraint("content <> ''"), nullable=False
    )

    def __init__(self, pmid: int, seq: int, name: str, content: str, label: str=None):
        assert pmid > 0
        assert seq > 0
        assert name in Section.SECTIONS, name
        assert content
        self.pmid = pmid
        self.seq = seq
        self.name = name
        self.label = label
        self.content = content

    def __str__(self):
        return '{}\t{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.seq), NULL(self.name),
            NULL(self.label), NULL(
                self.content.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
            )
        )

    def __repr__(self):
        return "Section<{}:{}>".format(self.pmid, self.seq)

    def __eq__(self, other):
        return isinstance(other, Section) and \
               self.pmid == other.pmid and \
               self.seq == other.seq and \
               self.name == other.name and \
               self.label == other.label and \
               self.content == other.content


class Medline(_Base):
    """
    A MEDLINE or PubMed record.

    Attributes:

        pmid
            the record's identifier (PubMed ID)
        status
            the current status of this record (see `Medline.STATES`)
        journal
            the journal name (Medline TA)
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

    __tablename__ = 'records'

    sections = relation(
        Section, backref='medline', cascade='all, delete-orphan',
        order_by=Section.__table__.c.seq
    )
    authors = relation(
        Author, backref='medline', cascade='all, delete-orphan',
        order_by=Author.__table__.c.pos
    )
    identifiers = relation(
        Identifier, backref='medline', cascade='all, delete-orphan',
        collection_class=column_mapped_collection(Identifier.namespace)
    )
    databases = relation(
        Database, backref='medline', cascade='all, delete-orphan',
    )
    chemicals = relation(
        Chemical, backref='medline', cascade='all, delete-orphan',
        order_by=Chemical.__table__.c.num
    )
    descriptors = relation(
        Descriptor, backref='medline', cascade='all, delete-orphan',
        order_by=Descriptor.__table__.c.num
    )
    qualifiers = relation(Qualifier, backref='medline')

    pmid = Column(BigInteger, CheckConstraint('pmid > 0'),
                  primary_key=True, autoincrement=False)
    status = Column(Enum(*STATES, name='state'), nullable=False)
    journal = Column(Unicode(length=256), CheckConstraint("journal <> ''"),
                     nullable=False)
    created = Column(Date, nullable=False)
    completed = Column(Date)
    revised = Column(Date)
    modified = Column(
        Date, default=date.today, onupdate=date.today, nullable=False
    )

    def __init__(self, pmid: int, status: str, journal: str,
                 created: date, completed: date=None, revised: date=None):
        assert pmid > 0
        assert status in Medline.STATES, status
        assert journal
        assert isinstance(created, date)
        assert completed is None or isinstance(completed, date)
        assert revised is None or isinstance(revised, date)
        self.pmid = pmid
        self.status = status
        self.journal = journal
        self.created = created
        self.completed = completed
        self.revised = revised

    def __str__(self):
        return '{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(
            NULL(self.pmid), NULL(self.status), NULL(self.journal),
            DATE(self.created), DATE(self.completed), DATE(self.revised),
            DATE(date.today() if self.modified is None else self.modified)
        )

    def __repr__(self):
        return "Medline<{}>".format(self.pmid)

    def __eq__(self, other):
        return isinstance(other, Medline) and \
               self.pmid == other.pmid and \
               self.status == other.status and \
               self.journal == other.journal and \
               self.created == other.created and \
               self.completed == other.completed and \
               self.revised == other.revised

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

            for tname in cls.TABLENAMES:
                if tname in data and len(data[tname]):
                    conn.execute(target_ins[tname], data[tname])

            transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            conn.close()

    @classmethod
    def select(cls, pmids: list, attributes: iter) -> iter([RowProxy]):
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
    def selectAll(cls, pmids: list) -> iter([RowProxy]):
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
    def existing(cls, pmids: list) -> set:
        "Return the sub- `set` of all *pmids* that exist in the DB."
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
    def missing(cls, pmids: list) -> set:
        "Return the sub- `set` of all *pmids* that do not exist in the DB."
        return set(pmids) - Medline.existing(pmids)

    @classmethod
    def modifiedBefore(cls, pmids: list, before: date) -> set:
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
