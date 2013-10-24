Synopsis
========

A tool to parse MEDLINE XML files or download eUtils' PubMed XML,
bootstrapping a MEDLINE/PubMed database store,
updating and/or deleting the records, and
writing the contents of selected PMIDs into flat-files.

Entity Relationship Model
=========================

::

    [Author] → [Medline] ← [Descriptor] ← [Qualifier]
                ↑     ↑
      [Identifier]   [Section]  [Database]  [Chemical]

Medline (records)
  **pmid**:BIGINT, *status*:ENUM(state), *journal*:VARCHAR(256),
  *created*:DATE, completed:DATE, revised:DATE, modified:DATE

Author (authors)
  **pmid**:FK(Medline), **pos**:SMALLINT, *name*:TEXT,
  initials:VARCHAR(128), forename:VARCHAR(128), suffix:VARCHAR(128),

Descriptor (descriptors)
  **pmid**:FK(Medline), **pos**:SMALLINT, *name*:TEXT, major:BOOL

Qualifier (qualifiers)
  **pmid**:FK(Descriptor), **pos**:FK(Descriptor), **sub**:SMALLINT,
  *name*:TEXT, major:BOOL

Identifier (identifiers)
  **pmid**:FK(Medline), **namespace**:VARCHAR(32), *value*:VARCHAR(256)

Database (databases)
  **pmid**:FK(Medline), **name**:VARCHAR(32), **accession**:VARCHAR(256)

Chemical (chemicals)
  **pmid**:FK(Medline), **num**:VARCHAR(32), uid:VARCHAR(256), *name*:VARCHAR(256)

Section (sections)
  **pmid**:FK(Medline), **seq**:SMALLINT, *name*:ENUM(section),
  label:VARCHAR(256), *content*:TEXT

- **bold** (Composite) Primary Key
- *italic* NOT NULL

Supported PubMed XML Elements
=============================

- PMID
- ArticleTitle (`Section.name` ``Title``)
- VernacularTitle (`Section.name` ``Vernacular``)
- AbstractText (`Section.name` ``Abstract`` or capitalized NlmCategory)
- CopyrightInformation (`Section.name` ``Copyright``)
- DescriptorName (`Descriptor.name` MeSH term descriptor)
- QualifierName (`Qualifier.name` MeSH term qualifier)
- Author (`Author`)
- ELocationID (`Identifier`)
- OtherID (`Identifier`)
- DataBankName (`Database.name`)
- AccessionNumber (`Database.accession`)
- SubstanceName (`Chemcial.name`)
- RegistryNumber (`Chemical.uid`)
- ArticleId (`Identifier`; only available in online PubMed XML)
- MedlineCitation (only Status; `Medline.status`)
- DateCompleted (`Medline.completed`)
- DateCreated (`Medline.created`)
- DateRevised (`Medline.revised`)
- MedlineTA (`Medline.journal`)

Requirements
============

- Python 3.2+
- SQL Alchemy 0.7+
- any database SQL Alchemy can work with

*Note* that while any SQL Alchemy DB will work, it is **strongly** discouraged
to use any other combination that PostgeSQL and psycogp2, because it is the
only combination in SQL Alchemy where data streaming from the DB actually
works. You can use other DBs for small MEDLINE collections, but in general,
for now, it is recommended to stick to this combo.

Notice: VersionID
=================

MEDLINE has began to use versions to allow publishers to add multiple citations
for the same PMID. This only occurs with 71 articles from one journal,
"PLOS Curr", in the 2013 baseline, creating a total of 149 non-unique records.

As this is the only journal and as there should only be one abstract per
publication in the database, alternative versions are currently being ignored.
In other words, if a MedlineCitation has a VersionID value, that records can
be skipped to avoid DB errors from non-unique records.

In short, this tool currently **only removes** alternate citations.

Setup
=====

If needed, install all dependencies/requirements::

    pip install argparse # only for python3 < 3.2
    pip install sqlalchemy
    pip install psycopg2 # optional, can use any other DB driver

Create the PostreSQL database (optional)::

    createdb medline 

Usage
=====

``medic [options] COMMAND PMID|FILE...``

The ``--url URL`` option representing the DSN of the database might
be needed (default: ``postgresql://localhost/medline``); For example:

Postgres
    ``postgresql://host//dbname``
SQLite
    ``sqlite:////absolute/path/to/foo.db`` or
    ``sqlite:///relative/path/to/foo.db``

The tool has five **COMMAND** options:

``insert``
    create records in the DB by parsing MEDLINE XML files or
    by downloading PubMed XML from NCBI eUtils for a list of PMIDs
``write``
    write records as plaintext files to a directory, each file named as
    "<pmid>.txt", and containing most of the DB stored content or just the
    TIAB (title and abstract)
``update``
    insert or update records in the DB (instead of creating them); note that
    if a record exists, but is added with ``create``, this would throw an
    `IntegrityError`. If you are not sure if the records are in the DB or
    not, use ``update`` (N.B. that ``update`` is slower).
``delete``
    delete records from the DB for a list of PMIDs
``parse``
    does not interact with the DB, but rather creates ".tab" files for each
    table that later can be used to load a database, particularly useful when
    bootstrapping a large collection

For example, to download two PubMed records by PMID and put them into
the DB::

    fnlmedline.py update 1000 123456

To insert a MEDLINE XML file into the DB::

    fnlmedline.py insert medline.xml

Read MEDLINE XML files for dumping large collections::

    fnlmedline.py parse medline*.xml.gz

Note that in the last example, because of the suffix ".gz", the parser
automatically decompresses the file(s) first. This feature *only*
works with GNU-zipped files and the ".gz" suffix must be present.

Therefore, command line arguments are treated as follows:

integer values
    are always treated as PMIDs to download PubMed XML data
all other values
    are always treated as MEDLINE XML files to parse
values ending in ".gz"
    are always treated as gzipped MEDLINE XML files

Loading the MEDLINE baseline
============================

Please be aware that the MEDLINE baseline **is not unique**, meaning that it
contains a few records multiple times (see the above notice about the VersionID).
For example, in the 2013 baseline, PMID 20029614 is present ten times in the
baseline, each version at a different stage of revision. Because it is the first
entry (in the order they appear in the baseline files) without a VersionID that
seems to be the relevant record, it is possible to filter these duplicates while
doing a ``parse``, ``insert`` or ``update`` by using the ``--uniq`` option. This
will simply skip citations that have a VersionID other than `1`.

To quickly load a parsed dump into a PostgreSQL DB on the same machine, do::

    for table in records descriptors qualifiers authors sections databases identifiers chemicals;
      do psql medline -c "COPY $table FROM '`pwd`/${table}.tab';";
    done
