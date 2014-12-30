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

from medic.orm import Citation, Abstract, Author, Chemical, Database, \
    Descriptor, Identifier, Keyword, PublicationType, Qualifier, Section

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
                logger.debug(
                    'detected a citation with VersionID "%s"',
                    version
                )
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
            try:
                for i in self.yieldFromGenerator(element):
                    yield i
            except Exception:
                logger.critical('error while parsing PMID %d', self.pmid)
                raise

    def yieldFromGenerator(self, element):
        logger.debug('processing %s', element.tag)
        instance = getattr(self, element.tag)(element)

        if instance is not None:
            logger.debug('parsed %s', element.tag)

            if isinstance(instance, types.GeneratorType):
                for i in instance:
                    if i is not None:
                        yield i
            else:
                yield instance
        else:
            logger.debug('ignored %s', element.tag)

    @staticmethod
    def DeleteCitation(element):
        for pmid in element.findall('PMID'):
            yield int(pmid.text)

    def MedlineCitation(self, element):
        options = {}

        for name, key in (('DateCompleted', 'completed'),
                          ('DateCreated', 'created'),
                          ('DateRevised', 'revised')):
            e = element.find(name)

            if e is not None:
                options[key] = ParseDate(e)

        created = options['created']
        del options['created']
        status = element.get('Status')
        journal = element.find('MedlineJournalInfo').find(
            'MedlineTA'
        ).text.strip()
        article = element.find('Article')
        title = article.find('ArticleTitle').text

        # ArticleTitle element tags should never be empty when in fact they are sometimes,
        # in violation of the MEDLINE DTD; For example, while PMID 22536004 has a
        # VernacularTitle, its ArticleTitle element is empty.
        if title is None and article.find('VernacularTitle') is not None:
            logging.info('PMID %d had no ArticleTitle; using VernacularTitle instead' % self.pmid)
            title = article.find('VernacularTitle').text

        if title is None:
            logging.warning('could not find any title for PMID %d' % self.pmid)
            title = 'UNKNOWN'  # NB: a DB constraint ensures titles are never empty!

        title = title.strip()
        pub_date = self.parsePubDate(
            article.find('Journal/JournalIssue/PubDate')
        )
        issue = self.parseIssue(article.find('Journal/JournalIssue'))
        pagination = self.parsePagination(
            article.find('Pagination/MedlinePgn')
        )

        if issue:
            options['issue'] = issue
        if pagination:
            options['pagination'] = pagination

        return Citation(self.pmid, status, title, journal,
                        pub_date, created, **options)

    @staticmethod
    def parsePubDate(element):
        medline = element.find('MedlineDate')

        if medline is not None:
            return medline.text.strip()
        else:
            datum = [element.find('Year').text.strip()]

            if element.find('Season') is not None:
                datum.append(element.find('Season').text.strip())
            elif element.find('Month') is not None:
                datum.append(element.find('Month').text.strip())

                if element.find('Day') is not None:
                    datum.append(element.find('Day').text.strip())

            return ' '.join(datum)

    @staticmethod
    def parseIssue(element):
        if element is not None:
            issue = None
            vol = element.find('Volume')
            iss = element.find('Issue')

            if vol is not None and vol.text:
                issue = vol.text.strip()

            if iss is not None and iss.text:
                if issue is None:
                    issue = iss.text.strip()
                else:
                    issue = '{}({})'.format(issue, iss.text.strip())

            return issue

    @staticmethod
    def parsePagination(element):
        if element is not None and element.text:
            return element.text.strip()

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

    TRUNC_MSG = re.compile(r'\(ABSTRACT TRUNCATED AT \d+ WORDS\)$')

    def __init__(self, *args, **kwargs):
        super(MedlineXMLParser, self).__init__(*args, **kwargs)
        self.seq = 0
        self.namespaces = None

    def reset(self, pmid):
        super(MedlineXMLParser, self).reset(pmid)
        self.namespaces = set()

    def Abstract(self, element):
        for item in self.parseAbstract(element, 'NLM'):
            yield item

    def parseAbstract(self, element, source):
        copy = element.find('CopyrightInformation')

        if copy is not None:
            text = copy.text.strip()
            copy = text if text else None

        # Note: yield Abstract before the Sections!
        yield Abstract(self.pmid, source, copy)
        self.seq = 0

        for child in element.getchildren():
            if child.text is not None and child.text.strip():
                if child.tag == 'AbstractText':
                    yield self.parseAbstractText(child, source)

    def parseAbstractText(self, element, source):
        self.seq += 1
        name = element.get('NlmCategory', 'Unassigned').capitalize()
        content = element.text.strip()
        label = element.get('Label', None)
        truncated = False

        if MedlineXMLParser.TRUNC_MSG.search(content):
            content = MedlineXMLParser.TRUNC_MSG.sub('', content).rstrip()
            truncated = True

            if not content:
                content = ' '  # truncated the whole section; use a space (placeholder)

        return Section(self.pmid, source, self.seq, name, content, label, truncated)

    def AuthorList(self, element):
        for pos, author in enumerate(element.getchildren()):
            yield self.parseAuthor(pos, author)

    def parseAuthor(self, pos, element):
        name, forename, initials, suffix = self.parseAuthorElements(
            element.getchildren()
        )

        if initials == forename and initials is not None:
            # prune the repetition of initials in the forename
            forename = None

        if name is not None:
            return Author(self.pmid, pos + 1, name, initials, forename, suffix)
        else:
            logger.warning(
                'empty or missing Author/LastName or CollectiveName in %i',
                self.pmid
            )
            return None

    def parseAuthorElements(self, children):
        name, forename, initials, suffix = None, None, None, None

        for child in children:
            if child.text is not None:
                text = child.text.strip()
                if child.tag == 'LastName':
                    name = text
                elif child.tag == 'ForeName':
                    forename = text[:256]  # safeguard for size violations
                elif child.tag == 'Initials':
                    initials = text
                elif child.tag == 'Suffix':
                    suffix = text
                elif child.tag == 'CollectiveName':
                    name = text
                    forename = ''
                    initials = ''
                    suffix = ''
                elif child.tag == 'Identifier':
                    pass
                elif child.tag == 'Affiliation':
                    pass
                elif child.tag == 'AffiliationInfo':
                    pass
                else:
                    logger.warning('unknown Author element %s "%s" in %i',
                                   child.tag, text, self.pmid)
            else:
                logger.warning('empty Author element %s in %i"',
                               child.tag, self.pmid)

        if forename and initials:
            if forename.replace(' ', '') == initials:
                forename = None

        return name, forename, initials, suffix

    def ChemicalList(self, element):
        for idx, chemical in enumerate(element.getchildren()):
            e = chemical.find('RegistryNumber')
            uid = None

            if e is not None and e.text is not None and e.text.strip() != "0":
                uid = e.text.strip()

            name = chemical.find('NameOfSubstance')
            yield Chemical(self.pmid, idx + 1, name.text.strip(), uid)

    def DataBank(self, element):
        name = element.find('DataBankName')

        if name is not None and name.text:
            accessions = {acc for acc in map(
                lambda e: e.text.strip() if e.text else None,
                element.find('AccessionNumberList').getchildren()
            ) if acc}

            for acc in accessions:
                yield Database(self.pmid, name.text, acc)

    def ELocationID(self, element):
        ns = element.get('EIdType').strip().lower()

        if ns not in self.namespaces:
            self.namespaces.add(ns)
            return Identifier(self.pmid, ns, element.text.strip())

    def KeywordList(self, element):
        owner = element.get('Owner', 'NLM').strip().upper()
        logger.debug('KeywordList Owner="%s"', owner)

        for cnt, keyword in enumerate(element.getchildren()):
            if keyword.text is not None:
                text = keyword.text.strip()

                if '\r' in text:
                    text = text[:text.find('\r')]

                if text:
                    yield Keyword(
                        self.pmid, owner, cnt + 1, text,
                        keyword.get('MajorTopicYN', 'N') == 'Y',
                    )

    def MedlineCitation(self, element):
        instance = Parser.MedlineCitation(self, element)
        element.clear()
        self.undefined()
        return instance

    def MeshHeadingList(self, element):
        for num, mesh in enumerate(element.getchildren()):
            descriptor = mesh.find('DescriptorName')

            if descriptor is not None and descriptor.text:
                yield self.parseDescriptor(num, descriptor)

            for sub, qualifier in enumerate(mesh.findall('QualifierName')):
                if qualifier.text:
                    yield self.parseQualifier(num, sub, qualifier)

    def parseDescriptor(self, num, element):
        return Descriptor(
            self.pmid, num + 1, element.text.strip(),
            element.get('MajorTopicYN', 'N') == 'Y',
        )

    def parseQualifier(self, num, sub, element):
        return Qualifier(
            self.pmid, num + 1, sub + 1, element.text.strip(),
            element.get('MajorTopicYN', 'N') == 'Y',
        )

    def OtherAbstract(self, element):
        children = element.getchildren()
        source = element.get('Type')

        # parse OtherAbstract only if it is not an abstract that only declares
        # that the publisher has another language version available.
        if (source != 'Publisher' or
            len(children) != 1 or
            children[0].tag != 'AbstractText' or
            children[0].text.strip() !=
                "Abstract available from the publisher."):
            for item in self.parseAbstract(element, source):
                yield item

    def OtherID(self, element):
        if element.get('Source', None) == 'NLM':
            text = element.text.strip()

            if text.startswith('PMC'):
                if 'pmc' not in self.namespaces:
                    self.namespaces.add('pmc')
                    return Identifier(self.pmid, 'pmc', text.split(' ', 1)[0])

    def PublicationType(self, element):
        if element.text:
            return PublicationType(self.pmid, element.text.strip().upper())

    # def VernacularTitle(self, element):
    #     if element.text is not None:
    #         self.seq += 1
    #         return Section(self.pmid, self.seq, 'Vernacular',
    #                        element.text.strip())


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
                logger.debug('skipping duplicate %s identifier "%s"', ns, text)
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
