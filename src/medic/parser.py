"""
.. py:module:: medic.parser
   :synopsis: An ORM parser for MEDLINE XML records.

.. moduleauthor: Florian Leitner <florian.leitner@gmail.com>
.. License: GNU Affero GPL v3 (http://www.gnu.org/licenses/agpl.html)
"""
import logging
import re
import struct
import types

from xml.etree.ElementTree import iterparse
from datetime import date

from medic.orm import \
        Medline, Section, Author, Descriptor, Qualifier, Database, Identifier, Chemical, Keyword

__all__ = ['MedlineXMLParser', 'PubMedXMLParser']

# translate three-letter month strings to integers:
MONTHS_SHORT = (None, 'jan', 'feb', 'mar', 'apr', 'may', 'jun',
                'jul', 'aug', 'sep', 'oct', 'nov', 'dec')

logger = logging.getLogger(__name__)


class State:
    UNDEFINED = 0
    PARSING = 1
    SKIPPING = -1


class Parser:
    """A basic parser implementation for NLM XML citations."""

    def __init__(self, unique=True):
        """
        Create a new parser.

        :param unique: if `True`, citations with VersionID != "1" are skipped
        """
        logger.info('configuring a %sunique %s',
                    "" if unique else "non-", self.__class__.__name__)
        self.unique = unique
        self.events = ('start', 'end') if unique else None
        logger.debug('state: UNDEFINED')
        self._state = State.UNDEFINED
        self.pmid = -1

    def reset(self, pmid):
        self.pmid = pmid

    def skipping(self):
        logger.debug('state: SKIPPING')
        self._state = State.SKIPPING

    def isSkipping(self):
        return State.SKIPPING == self._state

    def undefined(self):
        logger.debug('state: UNDEFINED')
        self._state = State.UNDEFINED

    def isUndefined(self):
        return State.UNDEFINED == self._state

    def parsing(self):
        logger.debug('state: PARSING')
        self._state = State.PARSING

    def isParsing(self):
        return State.PARSING == self._state

    def parse(self, xml_stream):
        try:
            for event, element in iterparse(xml_stream, self.events):
                if event == 'start':
                    self.startElement(element)
                else:
                    for instance in self.yieldInstances(element):
                        yield instance
        except struct.error:
            logger.exception('compressed gzip file is corrupt')

    def startElement(self, element):
        if self.unique and element.tag == 'MedlineCitation':
            version = element.get('VersionID')

            if version is not None and version.strip() != "1":
                logger.debug('detected a citation with VersionID "%s"', version)
                self.skipping()

    def yieldInstances(self, element):
        if element.tag == 'PMID':
            self.PMID(element)
        elif element.tag == 'DeleteCitation':
            for pmid in self.DeleteCitation(element):
                yield pmid
        elif self.isSkipping():
            if element.tag == 'MedlineCitation':
                self.undefined()
        elif hasattr(self, element.tag):
            logger.debug('processing %s: %s', element.tag, repr(element.text))
            instance = getattr(self, element.tag)(element)

            if instance is not None:
                logger.debug('parsed %s', element.tag)

                if isinstance(instance, types.GeneratorType):
                    for i in instance:
                        yield i
                else:
                    yield instance
            else:
                logger.debug('ignored %s', element.tag)


    def DeleteCitation(self, element):
        for pmid in element.findall('PMID'):
            yield int(pmid.text)

    def MedlineCitation(self, element):
        dates = {}

        for name, key in (('DateCompleted', 'completed'),
                          ('DateCreated', 'created'),
                          ('DateRevised', 'revised')):
            e = element.find(name)

            if e is not None:
                dates[key] = ParseDate(e)

        status = element.get('Status')
        journal = element.find('MedlineJournalInfo').find('MedlineTA').text.strip()
        return Medline(self.pmid, status, journal, **dates)

    def PMID(self, element):
        pmid = int(element.text)

        if self.isUndefined():
            logger.debug('parsing PMID %i', pmid)
            self.reset(pmid)
            self.parsing()
        elif self.isParsing():
            logger.debug('another PMID %i', pmid)
        elif self.isSkipping():
            logger.info('skipping PMID %i', pmid)
        else:
            logger.error('unknown state %i', self._state)


class MedlineXMLParser(Parser):
    """A parser for (offline) MEDLINE XML (files)."""

    def __init__(self, *args, **kwargs):
        super(MedlineXMLParser, self).__init__(*args, **kwargs)
        self.seq = 0
        self.namespaces = None

    def reset(self, pmid):
        super(MedlineXMLParser, self).reset(pmid)
        self.seq = 0
        self.namespaces = set()

    def AbstractText(self, element):
        # sometimes there are non-content AbstractText
        # elements in MEDLINE/PubMed ("<AbstractText/>")
        if element.text is not None:
            text = element.text.strip()
            # and, less frequently, they might only contain whitespaces
            if text:
                self.seq += 1
                section = element.get('NlmCategory', 'Abstract').capitalize()
                return Section(
                    self.pmid, self.seq, section, text, element.get('Label', None)
                )

        logger.debug('empty %s AbstractText in %i',
                     element.get('NlmCategory', 'ABSTRACT'), self.pmid)
        return None

    def ArticleTitle(self, element):
        if element.text is not None:
            self.seq += 1
            return Section(self.pmid, self.seq, 'Title', element.text.strip())

    def AuthorList(self, element):
        for pos, author in enumerate(element.getchildren()):
            yield self.parseAuthor(pos, author)

    def parseAuthor(self, pos, element):
        name, forename, initials, suffix = self.parseAuthorElements(element.getchildren())

        if initials == forename and initials is not None:
            # prune the repetition of initials in the forename
            forename = None

        if name is not None:
            return Author(self.pmid, pos + 1, name, initials, forename, suffix)
        else:
            logger.warning('empty or missing Author/LastName or CollectiveName in %i',
                           self.pmid)
            return None

    def parseAuthorElements(self, children):
        name, forename, initials, suffix = None, None, None, None

        for child in children:
            if child.text is not None:
                text = child.text.strip()
                if child.tag == 'LastName':
                    name = text
                elif child.tag == 'ForeName':
                    forename = text[:128]
                elif child.tag == 'Initials':
                    initials = text[:128]
                elif child.tag == 'Suffix':
                    suffix = text[:128]
                elif child.tag == 'CollectiveName':
                    name = text
                    forename = ''
                    initials = ''
                    suffix = ''
                elif child.tag == 'Identifier':
                    pass
                elif child.tag == 'Affiliation':
                    pass
                else:
                    logger.warning('unknown Author element %s "%s" in %i',
                                   child.tag, text, self.pmid)
            else:
                logger.warning('empty Author element %s in %i"', child.tag, self.pmid)

        return name, forename, initials, suffix

    def ChemicalList(self, element):
        for idx, chemical in enumerate(element.getchildren()):
            e = chemical.find('RegistryNumber')
            uid = None

            if e is not None and e.text is not None and e.text.strip() != "0":
                uid = e.text.strip()

            name = chemical.find('NameOfSubstance')
            yield Chemical(self.pmid, idx + 1, name.text.strip(), uid)

    def CopyrightInformation(self, element):
        if element.text is not None:
            self.seq += 1
            return Section(self.pmid, self.seq, 'Copyright', element.text.strip())

    def DataBank(self, element):
        name = element.find('DataBankName')

        if name is not None and name.text:
            done = set()

            for acc in element.find('AccessionNumberList').getchildren():
                if acc.text and acc.text not in done:
                    done.add(acc.text)
                    yield Database(self.pmid, name.text, acc.text)

    def ELocationID(self, element):
        ns = element.get('EIdType').strip().lower()

        if ns not in self.namespaces:
            self.namespaces.add(ns)
            return Identifier(self.pmid, ns, element.text.strip())

        return None

    def KeywordList(self, element):
        owner = element.get('Owner', 'NLM').strip().upper()
        logger.debug('KeywordList Owner="%s"', owner)

        for cnt, keyword in enumerate(element.getchildren()):
            if keyword.text is not None:
                yield Keyword(
                    self.pmid, owner, cnt + 1, keyword.text.strip(),
                    keyword.get('MajorTopicYN', 'N') == 'Y',
                )


    def MedlineCitation(self, element):
        instance = Parser.MedlineCitation(self, element)
        element.clear()
        self.undefined()
        return instance

    def MeshHeadingList(self, element):
        for num, mesh in enumerate(element.getchildren()):
            yield self.parseDescriptor(num, mesh.find('DescriptorName'))

            for sub, qualifier in enumerate(mesh.findall('QualifierName')):
                yield self.parseQualifier(num, sub, qualifier)

    def parseDescriptor(self, num, element):
        if element.text is not None:
            return Descriptor(
                self.pmid, num + 1, element.text.strip(),
                element.get('MajorTopicYN', 'N') == 'Y',
            )

    def parseQualifier(self, num, sub, element):
        if element.text is not None:
            return Qualifier(
                self.pmid, num + 1, sub + 1, element.text.strip(),
                element.get('MajorTopicYN', 'N') == 'Y',
            )

    def OtherID(self, element):
        if element.get('Source', None) == 'NLM':
            text = element.text.strip()

            if text.startswith('PMC'):
                if 'pmc' not in self.namespaces:
                    self.namespaces.add('pmc')
                    return Identifier(self.pmid, 'pmc', text.split(' ', 1)[0])

        return None

    def VernacularTitle(self, element):
        if element.text is not None:
            self.seq += 1
            return Section(self.pmid, self.seq, 'Vernacular', element.text.strip())


class PubMedXMLParser(MedlineXMLParser):
    """A parser for PubMed (eUtils, online) XML."""

    def __init__(self, *args, **kwargs):
        super(PubMedXMLParser, self).__init__(*args, **kwargs)

    def PubmedArticle(self, element):
        element.clear()
        self.undefined()

    def ArticleId(self, element):
        instance = None
        ns = element.get('IdType').strip().lower()
        text = element.text.strip()

        if ns in self.namespaces:
            if re.match('\d[\d\.]+/.+', element.text.strip()) and \
                    'doi' not in self.namespaces:
                self.namespaces.add('doi')
                instance = Identifier(self.pmid, 'doi', text)
            else:
                logger.info('skipping duplicate %s identifier "%s"', ns, text)
        else:
            self.namespaces.add(ns)
            instance = Identifier(self.pmid, ns, text)

        return instance

    def MedlineCitation(self, element):
        # skip MedlineXMLParser-specific instructions:
        return Parser.MedlineCitation(self, element)


def ParseDate(date_element):
    """Parse a **valid** date that (at least) has to have a Year element."""
    year = int(date_element.find('Year').text)
    month, day = 1, 1
    month_element = date_element.find('Month')
    day_element = date_element.find('Day')

    if month_element is not None:
        month_text = month_element.text.strip()
        try:
            month = int(month_text)
        except ValueError:
            logger.debug('non-numeric Month "%s"', month_text)
            try:
                month = MONTHS_SHORT.index(month_text.lower())
            except ValueError:
                logger.warning('could not parse Month "%s"', month_text)
                month = 1

    if day_element is not None:
        try:
            day = int(day_element.text)
        except (AttributeError, ValueError):
            logger.warning('could not parse Day "%s"', day_element.text)

    return date(year, month, day)
