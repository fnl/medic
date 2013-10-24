"""
.. py:module:: medic.web
   :synopsis: Downloading MEDLINE XML records via eUtils.

.. moduleauthor:: Florian Leitner <florian.leitner@gmail.com>
.. License: GNU Affero GPL v3 (http://www.gnu.org/licenses/agpl.html)
"""

import logging
from http.client import HTTPResponse  # function annotation only
from urllib.request import build_opener

URL_OPENER = build_opener()
# to avoid having to build a new opener for each request

logger = logging.getLogger(__name__)

EUTILS_URL = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?' \
             'tool=libfnl&db=pubmed&retmode=xml&rettype=medline&id='
"""
The eUtils URL to which requests for MEDLINE XML records can be made.
Multiple PMIDs may be appended comma-separated to this URL.
"""


def Download(pmids: list, timeout: int=60) -> HTTPResponse:
    """
    :param pmids: a list of PMIDs, but no more than `FETCH_SIZE`; values that
        can be cast to string
    :param timeout: seconds to wait for a response

    :return: an open XML stream

    :raises IOError: if the stream from eUtils cannot be opened
    :raises urllib.error.URLError: if the connection to the eUtils URL cannot
        be made
    :raises socket.timout: if *timeout* seconds have passed before a response
        arrives
    """
    assert len(pmids) <= 100, 'too many PMIDs'
    url = EUTILS_URL + ','.join(map(str, pmids))
    logger.info('fetching %i MEDLINE records from %s', len(pmids), url)
    return URL_OPENER.open(url, timeout=timeout)
