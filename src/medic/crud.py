"""
.. py:module:: medic.crud
   :synopsis: I/O CRUD to manage a MEDLINE DB.

.. moduleauthor:: Florian Leitner <florian.leitner@gmail.com>
.. License: GNU Affero GPL v3 (http://www.gnu.org/licenses/agpl.html)
"""
import logging

from itertools import chain
from gzip import open as gunzip
from os.path import join
from types import LambdaType
from sqlalchemy.exc import IntegrityError, DatabaseError
from sqlalchemy.orm import Session

from medic.orm import *
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
    "Return an iterator over all `Medline` records for the *PMIDs*."
    count = 0
    # noinspection PyUnresolvedReferences
    for record in session.query(Medline).filter(Medline.pmid.in_(pmids)):
        count += 1
        yield record
    logger.info("retrieved %i records", count)


# noinspection PyUnusedLocal
def delete(session: Session, pmids: list([int])) -> bool:
    "Delete all records for the *PMIDs*."
    # noinspection PyUnresolvedReferences
    count = session.query(Medline).filter(Medline.pmid.in_(pmids)).delete(
        synchronize_session=False
    )
    session.commit()
    logger.info("deleted %i records", count)
    return True


def dump(files: iter, output_dir: str, unique: bool):
    "Parse MEDLINE XML files into tabular flat-files for each DB table."
    out_stream = {
        Medline.__tablename__: open(join(output_dir, "records.tab"), "wt"),
        Section.__tablename__: open(join(output_dir, "sections.tab"), "wt"),
        Descriptor.__tablename__: open(join(output_dir, "descriptors.tab"), "wt"),
        Qualifier.__tablename__: open(join(output_dir, "qualifiers.tab"), "wt"),
        Author.__tablename__: open(join(output_dir, "authors.tab"), "wt"),
        Identifier.__tablename__: open(join(output_dir, "identifiers.tab"), "wt"),
        Database.__tablename__: open(join(output_dir, "databases.tab"), "wt"),
        Chemical.__tablename__: open(join(output_dir, "chemicals.tab"), "wt"),
        'delete': lambda: open(join(output_dir, "delete.txt"), "wt"),
    }
    count = 0
    parser = MedlineXMLParser(unique)

    for f in files:
        logger.info('dumping %s', f)

        if f.lower().endswith('.gz'):
            in_stream = gunzip(f, 'rb')
        else:
            in_stream = open(f)

        count += _dump(in_stream, out_stream, parser)

    if not isinstance(out_stream['delete'], LambdaType):
        out_stream['delete'].write('0));')

    for stream in out_stream.values():
        if not isinstance(stream, LambdaType):
            stream.close()

    logger.info("parsed %i records", count)


def _dump(in_stream, out_stream: dict, parser: Parser) -> int:
    logger.info('dumping %s', in_stream.name)
    count = 0

    for i in parser.parse(in_stream):
        if type(i) == int:
            if isinstance(out_stream['delete'], LambdaType):
                out_stream['delete'] = out_stream['delete']()
                out_stream['delete'].write(
                    'DELETE FROM {} WHERE pmid = ANY (VALUES ('.format(
                        Medline.__tablename__
                    )
                )
            out_stream['delete'].write(str(i))
            out_stream['delete'].write('), (')
        else:
            out_stream[i.__tablename__].write(str(i))

            if i.__tablename__ == Medline.__tablename__:
                count += 1

    return count


def _add(session: Session, files_or_pmids: iter, dbHandle, unique=True):
    pmids = []
    count = 0
    initial = session.query(Medline).count() if logger.isEnabledFor(logging.INFO) else 0

    try:
        for arg in files_or_pmids:
            try:
                pmids.append(int(arg))
            except ValueError:
                logger.info("parsing %s", arg)
                count += _streamInstances(session, _fromFile(arg, unique), dbHandle)

        if len(pmids):
            count += _downloadAll(session, pmids, unique, dbHandle)

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


def _streamInstances(session: Session, stream: iter, handle) -> int:
    count = 0
    deletion = []

    for citation in _collectCitation(stream):
        if type(citation) == int:
            deletion.append(citation)
        else:
            count += _handleCitation(handle, citation)

    if deletion:
        delete(session, deletion)

    return count


def _collectCitation(stream: iter) -> list:
    buffer = []

    for instance in stream:
        if type(instance) == int:
            yield instance
        else:
            buffer.append(instance)

            if isinstance(instance, Medline):
                yield buffer

    if len(buffer):
        yield buffer


def _handleCitation(handle, instances: list):
    while instances:
        handle(instances.pop())

    return 1


def _downloadAll(session: Session, pmids: list, unique: bool, handle) -> int:
    parser = PubMedXMLParser(unique)
    downloads = map(Download, [pmids[100 * i:100 * i + 100] for i in range(len(pmids) % 100)])
    streams = map(parser.parse, downloads)
    return _streamInstances(session, chain(streams), handle)


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


