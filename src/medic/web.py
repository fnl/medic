"""
.. py:module:: medic.web
   :synopsis: Downloading MEDLINE XML records via eUtils.

.. moduleauthor:: Florian Leitner <florian.leitner@gmail.com>
.. License: GNU Affero GPL v3 (http://www.gnu.org/licenses/agpl.html)
"""

import logging

try:
    # noinspection PyCompatibility,PyUnresolvedReferences
    from http.client import HTTPResponse  # only used in function annotations
    # noinspection PyCompatibility,PyUnresolvedReferences
    from urllib.request import build_opener
except ImportError:
    # Python 2.7 compatibility
    # noinspection PyCompatibility,PyUnresolvedReferences
    from httplib import HTTPResponse
    # noinspection PyCompatibility,PyUnresolvedReferences
    from urllib2 import build_opener

from io import StringIO

URL_OPENER = build_opener()
# to avoid having to build a new opener for each request

logger = logging.getLogger(__name__)

EUTILS_URL = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?' \
             'tool=medic&db=pubmed&retmode=xml&rettype=medline&id='
"""
The eUtils URL to which requests for MEDLINE XML records can be made.
Multiple PMIDs may be appended comma-separated to this URL.
"""

DATABANK_LINK = {
    'DOI':
    "http://dx.doi.org/",
    'GDB': "#",
    'GENBANK':
    "http://www.ncbi.nlm.nih.gov/nucgss/",
    'OMIM':
    "http://omim.org/entry/",
    'PDB':
    "http://www.rcsb.org/pdb/explore/explore.do?structureId=",
    'PIR': "#",
    'PMC':
    "http://www.ncbi.nlm.nih.gov/pmc/articles/",
    'RefSeq':
    "http://www.ncbi.nlm.nih.gov/nuccore/",
    'SWISSPROT':
    "http://www.uniprot.org/uniprot/",
    'ClinicalTrials.gov': "#",
    'ISRCTN': "#",
    'GEO': "#",
    'PubChem-Substance':
    "http://pubchem.ncbi.nlm.nih.gov/summary/summary.cgi?sid=",
    'PubChem-Compound':
    "http://pubchem.ncbi.nlm.nih.gov/summary/summary.cgi?cid=",
    'PubChem-BioAssay':
    "http://pubchem.ncbi.nlm.nih.gov/assay/assay.cgi?aid=",
    'PubMed':
    "http://www.ncbi.nlm.nih.gov/pubmed/",
}
"""
Base links to external resources; link target ID needs to be attached to tail.
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
    :raises socket.timeout: if *timeout* seconds have passed before a response
        arrives
    """
    assert len(pmids) <= 100, 'too many PMIDs'
    url = EUTILS_URL + ','.join(map(str, pmids))
    logger.info('fetching %i MEDLINE records from %s', len(pmids), url)
    return URL_OPENER.open(url, timeout=timeout)


def FormatHTML(query):
    file = StringIO()
    p = file.write
    p("""<!doctype html>
<html><head>
  <meta charset="UTF-8"/>
  <title>PubMed Articles</title>
  <script>
function toggle(e) {
    if (e.style.display == 'none')
        e.style.display = 'block'
    else
        e.style.display = 'none'
}
function toggleAll(items) {
    for (var i = items.length; i--; i != 0) {
      toggle(items.item(i))
    }
}
function toggleVisibility(pmid, target) {
    var metadata = document.getElementById(
      pmid
    ).getElementsByTagName('div').item(0)
    var elements = metadata.childNodes;
    for (i = elements.length; i--; i != 0) {
      var e = elements.item(i)
      if (typeof(e.getAttribute) != 'undefined' &&
          e.getAttribute("class") == target) {
        toggle(e)
      }
    }
}
  </script>
</head><body>""")  # .format(file.encoding))

    href = lambda pmid: "{}{}".format(DATABANK_LINK['PubMed'], pmid)
    button = (
        lambda pmid, target, title:
        '<button onclick="toggleVisibility({}, \'{}\')">{}</button>'.format(
            pmid, target, title
        )
    )
    doi = lambda val: '<a href="{}{}">{}</a>'.format(
        DATABANK_LINK['DOI'], val, val
    )
    pmc = (
        lambda val:
        '<a href="{}{}">{}</a>'.format(DATABANK_LINK['PMC'], val, val)
    )
    # dates = lambda rec: 'created: {}{}{}'.format(
    #     rec.created,
    #     "" if rec.completed is None else
    #     ", completed: {}".format(rec.completed),
    #     "" if rec.revised is None else
    #     ", revised: {}".format(rec.revised))
    formatMajor = lambda e: '<b>{}</b>'.format(e.name) if e.major else e.name

    for rec in query:
        logging.debug('writing PMID %i as HTML', rec.pmid)
        citation = '{}, {}'.format(rec.journal, rec.citation())
        link = None
        pt_list = ', '.join(pt.value for pt in rec.publication_types)
        p('<article id={}>'.format(rec.pmid))

        if 'doi' in rec.identifiers:
            link = '<a href="{}{}">'.format(
                DATABANK_LINK['DOI'], rec.identifiers['doi'].value
            )
        elif 'pmc' in rec.identifiers:
            link = '<a href="{}{}">'.format(
                DATABANK_LINK['PMC'], rec.identifiers['pmc'].value
            )

        if link:
            p('<p class="citation"><small>{}{}</a> '
              '({}, PMID:{})</small></p>'.format(
                  link, citation, pt_list, rec.pmid
              ))
        else:
            p('<p class="citation"><small>{} '
              '({}, PMID:{})</small></p>'.format(
                  citation, pt_list, rec.pmid
              ))

        p('  <h1><a href="{}">{}</a></h1>\n  <ol>'.format(
            href(rec.pmid), rec.title
        ))

        for author in rec.authors:
            p('    <li>{}</li>'.format(author.fullName()))

        p('  </ol>')

        for source in rec.abstracts:
            abstract = rec.abstracts[source]
            p('  <h3>{} Abstract</h3>'.format(source))

            for sec in abstract.sections:
                p('  <p title="{}">{}{}</p>'.format(
                    sec.name, "" if sec.label is None else
                    '{}<br/>'.format(sec.label.upper()), sec.content))

            if abstract.copyright:
                p('  <p title="Copyright"><small>{}</small></p>'.format(
                    abstract.copyright
                ))

        p('  <div title="Metadata">')

        if rec.descriptors:
            p('    {}'.format(button(rec.pmid, "mesh", "MeSH Terms")))
        if rec.keywords:
            p('    {}'.format(button(rec.pmid, "kwds", "Keywords")))
        if rec.chemicals:
            p('    {}'.format(button(rec.pmid, "chem", "Chemicals")))
        if rec.databases:
            p('    {}'.format(button(rec.pmid, "xref", "DB Links")))
        if rec.identifiers:
            p('    {}'.format(button(rec.pmid, "ids", "Article IDs")))

        if rec.descriptors:
            p('    <dl class="mesh">')

            for desc in rec.descriptors:
                p('      <dt>{}</dt><dd><ol>'.format(formatMajor(desc)))

                if desc.qualifiers:
                    for qual in desc.qualifiers:
                        p('        <li>{}</li>'.format(formatMajor(qual)))

                p('      </ol></dd>')
            p('    </dl>')

        if rec.chemicals:
            p('    <ul class="chem">')

            for chem in rec.chemicals:
                p('      <li>{}{}</li>'.format(
                    chem.name,
                    "" if chem.uid is None else
                    " ({})".format(chem.uid)
                ))

            p('    </ul>')

        if rec.keywords:
            p('    <dl class="kwds">')
            owner = None

            for kwd in rec.keywords:
                if kwd.owner != owner:
                    if owner is not None:
                        p('        </dd>')

                    p('      <dt>{}</dt><dd>'.format(kwd.owner))
                    owner = kwd.owner

                p('      <li>{}</li>'.format(formatMajor(kwd)))

            p('    </dd></dl>')

        if rec.databases:
            p('    <ul class="xref">')

            for xref in rec.databases:
                try:
                    p('      <li>{} <a href="{}{}">{}</a></li>'.format(
                        xref.name, DATABANK_LINK[xref.name],
                        xref.accession, xref.accession
                    ))
                except KeyError:
                    logging.error('unknown DB name: "{}"'.format(
                        xref.name
                    ))

            p('    </ul>')

        if rec.identifiers:
            p('    <ul class="ids">')

            for ns, i in rec.identifiers.items():
                if ns == 'doi':
                    p('      <li>{}</li>'.format(doi(i.value)))
                elif ns == 'pmc':
                    p('      <li>{}</li>'.format(pmc(i.value)))
                else:
                    p('      <li>{}:{}</li>'.format(ns, i.value))

            p('    </ul>')

        p('  </div>\n</article><hr/>')
    p("""  <script>
window.onload = function() {
    toggleAll(document.getElementsByTagName("ul"))
    toggleAll(document.getElementsByTagName("dl"))
}
  </script>
</body></html>""")
    return file.getvalue()
