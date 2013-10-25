"""
.. py:module:: medic.crud
   :synopsis: I/O CRUD to manage a MEDLINE DB.

.. moduleauthor:: Florian Leitner <florian.leitner@gmail.com>
.. License: GNU Affero GPL v3 (http://www.gnu.org/licenses/agpl.html)
"""
import logging

from functools import partial
from itertools import chain
from gzip import open as gunzip
from os import remove
from os.path import join
from sqlalchemy.exc import IntegrityError, DatabaseError
from sqlalchemy.orm import Session

from medic.orm import \
        Medline, Section, Author, Descriptor, Qualifier, Database, Identifier, Chemical
from medic.parser import MedlineXMLParser, PubMedXMLParser, Parser
from medic.web import Download

logger = logging.getLogger(__name__)


def insert(session: Session, files_or_pmids: iter, uniq: bool) -> bool:
    "Insert all records by parsing the *files* or downloading the *PMIDs*."
    _add(session, files_or_pmids, lambda i: session.add(i), uniq)


def update(session: Session, files_or_pmids: iter, uniq: bool) -> bool:
    "Update all records in the *files* (paths) or download the *PMIDs*."
    _add(session, files_or_pmids, lambda i: session.merge(i), uniq)


def select(session: Session, pmids: list([int])) -> iter([Medline]):
    "Return an iterator over all `Medline` records for a list of *PMIDs*."
    count = 0
    for record in session.query(Medline).filter(Medline.pmid.in_(pmids)):
        count += 1
        yield record
    logger.info("retrieved %i records", count)


def delete(session: Session, pmids: list([int])) -> bool:
    "Delete all records for a list of *PMIDs*."
    count = session.query(Medline).filter(Medline.pmid.in_(pmids)).delete(
        synchronize_session=False
    )
    session.commit()
    logger.info("deleted %i records", count)
    return True


def dump(files: iter, output_dir: str, unique: bool, update: bool):
    """
    Parse MEDLINE XML files into tabular flat-files for each DB table.

    In addtion, a ``delete.txt`` file is generated, containing the PMIDs
    that should first be deleted from the DB before copying the dump.

    :param files: a list of XML files to parse (optionally, gzipped)
    :param output_dir: path to the output directory for the dump
    :param unique: if ``True`` only VersionId == "1" records are dumped
    :param update: if ``True`` the PMIDs of all parsed records are
                   added to the list of PMIDs for deletion
    """
    out_stream = {
        Medline.__tablename__: open(join(output_dir, "records.tab"), "wt"),
        Section.__tablename__: open(join(output_dir, "sections.tab"), "wt"),
        Descriptor.__tablename__: open(join(output_dir, "descriptors.tab"), "wt"),
        Qualifier.__tablename__: open(join(output_dir, "qualifiers.tab"), "wt"),
        Author.__tablename__: open(join(output_dir, "authors.tab"), "wt"),
        Identifier.__tablename__: open(join(output_dir, "identifiers.tab"), "wt"),
        Database.__tablename__: open(join(output_dir, "databases.tab"), "wt"),
        Chemical.__tablename__: open(join(output_dir, "chemicals.tab"), "wt"),
        'delete': open(join(output_dir, "delete.txt"), "wt"),
    }
    count = 0
    parser = MedlineXMLParser(unique)

    for f in files:
        logger.info('dumping %s', f)

        if f.lower().endswith('.gz'):
            in_stream = gunzip(f, 'rb')
        else:
            in_stream = open(f)

        count += _dump(in_stream, out_stream, parser, update)

    for stream in out_stream.values():
        if stream.tell() == 0:
            stream.close()
            remove(join(output_dir, stream.name))
        else:
            stream.close()

    logger.info("parsed %i records", count)


def _dump(in_stream, out_stream: dict, parser: Parser, update: bool) -> int:
    logger.info('dumping %s', in_stream.name)
    count = 0

    for i in parser.parse(in_stream):
        if type(i) == int:
            print(i, file=out_stream['delete'])
        else:
            out_stream[i.__tablename__].write(str(i))

            if i.__tablename__ == Medline.__tablename__:
                count += 1

                if update:
                    print(i.pmid, file=out_stream['delete'])

    return count


def _add(session: Session, files_or_pmids: iter, dbHandle, unique: bool=True):
    pmids = []
    count = 0
    initial = session.query(Medline).count() if logger.isEnabledFor(logging.INFO) else 0

    try:
        for arg in files_or_pmids:
            try:
                pmids.append(int(arg))
            except ValueError:
                logger.info("parsing %s", arg)
                count += _streamInstances(session, dbHandle, _fromFile(arg, unique))

        if len(pmids):
            count += _downloadAll(session, dbHandle, pmids, unique)

        session.commit()

        if logger.isEnabledFor(logging.INFO):
            final = session.query(Medline).count()
            logger.info('parsed %i entities (records before/after: %i/%i)',
                        count, initial, final)
        return True
    except IntegrityError:
        logger.exception('DB integrity violated')
        session.rollback()
        return False
    except DatabaseError:
        logger.exception('adding records failed')
        if session.dirty:
            session.rollback()
        return False


def _streamInstances(session: Session, handle, stream: iter) -> int:
    """
    Stream citations and delete records in DB.

    If PMIDs (integers) are encountered on the stream, they are deleted after
    all citations have been handled.

    :param session: the DB session object (SQL Alchemy)
    :param handle: a DB handle to send ORM instances
    """
    count = 0
    deletion = []

    for citation in _collectCitation(stream):
        if type(citation) == int:
            deletion.append(citation)
        else:
            count += _handleCitation(handle, citation)

    if deletion:
        delete(session, deletion)

    logging.debug("streamed %i citations", count)
    return count


def _collectCitation(stream: iter) -> iter:
    "Collect PMIDs or whole citation lists from the stream."
    citation = []
    pmid = None

    for instance in stream:
        if type(instance) == int:
            logger.debug("delete PMID %i", instance)
            yield instance
        else:
            if instance.pmid != pmid:
                if citation:
                    yield citation
                    citation.clear()

                pmid = instance.pmid
                logger.debug("collecting PMID %i", pmid)

            citation.append(instance)

    if len(citation):
        yield citation


def _handleCitation(handle, instances: list):
    "Handle a list of instances representing a citation."
    # handle Medline first:
    for idx in range(len(instances)):
        if isinstance(instances[idx], Medline):
            handle(instances.pop(idx))
            break

    # and everythin else after that:
    while instances:
        handle(instances.pop())

    return 1


def _downloadAll(session: Session, dbHandle, pmids: list, unique: bool=True) -> int:
    """
    Download PubMed XML for a list of PMIDs (integers), parse the streams,
    and send the ORM instances to a DB handle.

    :param session: the SQL Alchemy DB session
    :param dbHandle: a function that takes one instance and sends it to the DB
    :param pmids: the list of PMIDs to download
    :param unique: if ``True``, only VersionID == "1" records are handled.
    """
    parser = PubMedXMLParser(unique)
    pmid_sets = [pmids[100 * i:100 * i + 100] for i in range(len(pmids) // 100 + 1)]
    downloads = map(Download, pmid_sets)
    instances = map(parser.parse, downloads)
    streaming = partial(_streamInstances, session, dbHandle)
    return sum(map(streaming, chain(instances)))


def _fromFile(name: str, unique: bool) -> iter:
    logger.info("parsing %s", name)
    parser = MedlineXMLParser(unique)
    stream = _openFile(name)
    return parser.parse(stream)


def _openFile(name):
    if name.lower().endswith('.gz'):
        # use wrapper to support pre-3.3
        return gunzip(name, 'rb')
    else:
        return open(name)
