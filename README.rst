=====
medic
=====
-------------------------------------------------
a command-line tool to manage a mirror of MEDLINE
-------------------------------------------------

The Swiss Army knife to parse MEDLINE_ XML files or
download eUtils' PubMed_ XML records,
bootstrapping a local MEDLINE/PubMed database,
updating and/or deleting the records, and
writing the contents of selected PMIDs into flat-files.

Synopsis
========

::

  medic [options] CMD FILE|PMID...

  medic parse baseline/medline*.xml.gz
  medic --update parse update/medline*.xml.gz
  medic --pmid-lists delete delete.txt
  medic --url sqlite://tmp.db insert pubmed.xml
  medic --pmid-lists update changed_pmids.txt
  medic --all update pubmed.xml
  medic --format html --output /var/www/medline.html write 2874014 1028734 1298474

Setup
=====

If you are **not** using ``pip install medic``, install all
dependencies/requirements::

  pip install sqlalchemy
  # only if using python3 < 3.2:
  pip install argparse 

Install the **DB driver** you prefer to use (supported are PostgreSQL
and SQLite, with the latter part of the Python StdLib)::

  pip install psycopg2 

Create the PostreSQL database::

  createdb medline 

If you are fine working with SQLite, you only need to use the path to the
SQLite DB file in the URL option::

  medic insert --url sqlite:///tmp.db 123456

Description
===========

``medic [options] COMMAND PMID|FILE...``

The ``--url URL`` option represents the DSN of the database and might
be needed (default: ``postgresql://localhost/medline``); For example:

PostgreSQL
  ``postgresql://host//dbname``
SQLite DB
  ``sqlite:////absolute/path/to/foo.db`` or
  ``sqlite:///relative/path/to/foo.db``

The five **COMMAND** arguments:

``insert``
  Create records in the DB by parsing MEDLINE XML files or
  by downloading PubMed XML from NCBI eUtils for a list of PMIDs.
``write``
  Write records as MEDLINE_ files to a directory, each file named as
  "<pmid>.txt". Alternatively, just the TIAB (title and abstract) plain-text
  can be output, and finally, a single file in TSV or HTML format can be
  generated (see option ``--format``).
``update``
  Insert or update records in the DB (instead of creating them); note that
  if a record exists, but is added with ``create``, this would throw an
  `IntegrityError`. If you are not sure if the records are in the DB or
  not, use ``update`` (N.B. that ``update`` is slower).
``delete``
  Delete records from the DB for a list of PMIDs (use ``--pmid-lists``!)
``parse``
  Does not interact with the DB, but rather creates ".tab" files for each
  table that later can be used to load a database, particularly useful when
  bootstrapping a large collection.

For example, to download two PubMed records by PMID and update them in
the DB::

  medic update 100000 123456

Add a single MEDLINE or PubMed XML file to the database::

  medic insert pudmed.xml

Export a few records from the database as HTML (to STDOUT)::

  medic write --format html 292837491 128374 213487

Note that if the suffix ".gz" is present, the parser automatically
decompresses the XML file(s) first. This feature *only* works with
GNU-zipped files and the ".gz" suffix must be present.

Therefore, command line arguments are treated as follows:

integer values
  are always treated as PMIDs to download PubMed XML data
all other values
  are always treated as MEDLINE XML files to parse
  **unless** you use the option ``--pmid-lists``
files ending in ".gz"
  are always treated as gzipped MEDLINE XML files

Requirements
============

- Python 3.2+
- SQL Alchewy 0.8+
- PostgreSQL 8.4+ or SQLite 3.7+

*Note* that while any SQL Alchemy DB might work, it is **strongly** discouraged
to use any other combination that PostgeSQL and psycogp2 or SQLite and the
Python STDLIB drivers, because it has not been tested.

Loading MEDLINE
===============

Please be aware that the MEDLINE distribution **is not unique**, meaning that
it contains a few records multiple times (see the section about
**Version IDs**).

Parsing and loading the baseline into a PostgreSQL DB on the same machine::

  medic parse baseline/medline14n*.xml.gz

  for table in records descriptors qualifiers authors sections \
  databases identifiers chemicals keywords publication_types;
    do psql medline -c "COPY $table FROM '`pwd`/${table}.tab';";
  done

For the update files, you need to go *one-by-one*, adding each one *in order*,
and using the flag ``--update`` when parsing the XML. After parsing an XML file
and *before* loading the dump, run ``medic delete --pmid-lists delete.txt``
to get rid of all entities that will be updated or should be removed (PMIDs
listed as ``DeleteCitation``\ s)::

  # parse a MEDLINE update file:
  medic --update parse medline14n1234.xml.gz

  # delete its updated and DeleteCitation records:
  medic --pmid-lists delete delete.txt

  # load (COPY) all tables for that MEDLINE file:
  for table in records descriptors qualifiers authors sections \
  databases identifiers chemicals keywords publication_types; 
    do psql medline -c "COPY $table FROM '`pwd`/${table}.tab';";
  done

Version IDs
===========

MEDLINE has began to use versions to allow publishers to add multiple citations
for the same PMID. This only occurs with 71 articles from one journal,
"PLOS Curr", in the 2013 baseline, creating a total of 149 non-unique records.

As this is the only journal and as there should only be one abstract per
publication in the database, alternative versions are currently being ignored.
In other words, if a MedlineCitation has a VersionID value, that records can
be skipped to avoid DB errors from non-unique records.

For example, in the 2013 baseline, PMID 20029614 is present ten times in the
baseline, each version at a different stage of revision. Because it is the
first entry (in the order they appear in the baseline files) without a
``VersionID`` or a version of "1" that so far is the relevant record,
``medic`` by default filters citations with other versions than "1". If you
do want to process other versions of a citation, use the option ``--all``.

In short, this tool by default **removes** alternate citations.

Database Tables
===============

Medline (records)
  **pmid**:BIGINT, *status*:ENUM(state), *journal*:VARCHAR(256),
  *pub_date*:VARCHAR(256), issue:VARCHAR(256), pagination:VARCHAR(256),
  *created*:DATE, completed:DATE, revised:DATE, modified:DATE

Section (sections)
  **pmid**:FK(Medline), **seq**:SMALLINT, *name*:ENUM(section),
  label:VARCHAR(256), *content*:TEXT

Author (authors)
  **pmid**:FK(Medline), **pos**:SMALLINT, *name*:TEXT,
  initials:VARCHAR(128), forename:VARCHAR(128), suffix:VARCHAR(128),

PublicationType (publication_types)
  **pmid**:FK(Medline), **value**:VARCHAR(256)

Descriptor (descriptors)
  **pmid**:FK(Medline), **num**:SMALLINT, major:BOOL, *name*:TEXT

Qualifier (qualifiers)
  **pmid**:FK(Descriptor), **num**:FK(Descriptor), **sub**:SMALLINT, major:BOOL, *name*:TEXT

Identifier (identifiers)
  **pmid**:FK(Medline), **namespace**:VARCHAR(32), *value*:VARCHAR(256)

Database (databases)
  **pmid**:FK(Medline), **name**:VARCHAR(32), **accession**:VARCHAR(256)

Chemical (chemicals)
  **pmid**:FK(Medline), **idx**:VARCHAR(32), uid:VARCHAR(256), *name*:VARCHAR(256)

Keyword (keywords)
  **pmid**:FK(Medline), **owner**:ENUM(owner), **cnt**:SMALLINT, major:BOOL, *value*:TEXT

- **bold** (Composite) Primary Key
- *italic* NOT NULL (Strings that may not be NULL are also never empty.)

Supported XML Elements
======================

Entities
--------

- The citation (``Medline`` and ``Identifier``)
- Title, Abstract, and Copyright (``Section``)
- Author (``Author``)
- Chemical (``Chemcial``)
- DataBank (``Database``)
- Keyword (``Keyword``)
- MeshHeading (``Descriptor`` and ``Qualifier``)
- PublicationType (``PublicationType``)
- DeleteCitation (for deleting records when parsing updates)

Fields/Values
-------------

- AbstractText (``Section.name`` "Abstract" or the *NlmCategory*, ``Section.content`` with *Label* as ``Section.label``)
- AccessionNumber (``Database.accession``)
- ArticleId (``Identifier.value`` with *IdType* as ``Identifier.namesapce``; only available in online PubMed XML)
- ArticleTitle (``Section.name`` "Title", ``Section.content``)
- CollectiveName (``Author.name``)
- CopyrightInformation (``Section.name`` "Copyright", ``Section.content``)
- DataBankName (``Database.name``)
- DateCompleted (``Medline.completed``)
- DateCreated (``Medline.created``)
- DateRevised (``Medline.revised``)
- DescriptorName (``Descriptor.name`` with *MajorTopicYN* as ``Descriptor.major``)
- ELocationID (``Identifier.value`` with *EIdType* as ``Identifier.namespace``)
- ForeName (``Author.forename``)
- Initials (``Author.initials``)
- Issue (``Medline.issue``)
- Keyword (``Keyword.value`` with *Owner* as ``Keyword.owner`` and *MajorTopicYN* as ``Keyword.major``)
- LastName (``Author.name``)
- MedlineCitation (only *Status* as ``Medline.status``)
- MedlineTA (``Medline.journal``)
- NameOfSubstance (``Chemcial.name``)
- MedlinePgn (``Medline.pagination``)
- OtherID (``Identifier.value`` iff *Source* is "PMC" with ``Identifier.namespace`` as "pmc")
- PMID (``Medline.pmid``)
- PubDate (``Medline.pub_date``)
- PublicationType (``PublicationType.value``)
- QualifierName (``Qualifier.name`` with *MajorTopicYN* as ``Qualifier.major``)
- RegistryNumber (``Chemical.uid``)
- Suffix (``Author.suffix``)
- VernacularTitle (``Section.name`` "Vernacular", ``Section.content``)
- Volume (``Medline.issue``)

Version History
===============

2.0.1
  - fixed a bug that lead to skipping of abstracts (thanks to Chris Roeder for detecting the issue)
2.0.0
  - added Keywords and PublicationTypes
  - added MEDLINE publication date, volume, issue, and pagination support
  - added MEDLINE output format and made it the default
  - DB structure change: descriptors.major and qualifiers.major columns swapped
  - DB structure change: section.name is now an untyped varchar (OtherAbstract separation)
  - cleaned up the ORM test cases
1.1.1
  - code cleanup (PEP8, PyFlake)
  - fixed an issue where the parser would not leave the skipping state
1.1.0
  - ``--update parse`` now writes a file to use with ``--pmid-lists delete``
  - fixed a bug with CRUD manager
  - added a man page
1.0.2
  - fixes to make the PyPi version and ``pip install medic`` work
1.0.1
  - updates to the setup.py and README.rst files
1.0.0
  - initial release

Copyright and License
=====================

License: `GNU GPL v3`_\ .
Copryright 2012, 2013 Florian Leitner. All rights reserved.

.. _GNU GPL v3: http://www.gnu.org/licenses/gpl-3.0.html
.. _MEDLINE: http://www.nlm.nih.gov/bsd/mms/medlineelements.html
.. _PubMed: http://www.ncbi.nlm.nih.gov/pubmed
