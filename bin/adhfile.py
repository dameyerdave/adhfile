#!/usr/bin/env python

import sys, time, re, os, sys
from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
import splunklib.client
from csv import reader
from datetime import datetime
import dateutil.parser
from itertools import chain
import backports.configparser as configparser
from file_read_backwards import FileReadBackwards

@Configuration()
class AdhfileCommand(GeneratingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    ##TODO:
    - datetime parser -- ok
    - field extraction by config -- ok
    - multiline support
    - caching in memory
    - field aliases -- ok
    - paging -- ok

    """

    file = Option(require=True)
    page = Option(require=False, default=0, validate=validators.Integer())
    psize = Option(require=False, default=10, validate=validators.Integer())
    sourcetype = Option(require=False, default='adhfile')
    grep = Option(require=False, default=None)

    kv = re.compile(r"\b(\w+)\s*?=\s*([^=]*)(?=\s+\w+\s*=|$)")
    re_alias = re.compile(r"(\w+) as (\w+)")

    # Add more strings that confuse the parser in the list
    UNINTERESTING = set(chain(dateutil.parser.parserinfo.JUMP,
                          dateutil.parser.parserinfo.PERTAIN,
                          ['a']))

    def _get_date(self, tokens):
        for end in xrange(len(tokens), 0, -1):
            region = tokens[:end]
            if all(token.isspace() or token in self.UNINTERESTING
                   for token in region):
                continue
            text = ''.join(region)
            try:
                date = dateutil.parser.parse(text)
                return end, date
            except ValueError:
                pass

    def find_dates(self, text, max_tokens=50, allow_overlapping=False):
        tokens = filter(None, re.split(r'(\S+|\W+)', text))
        skip_dates_ending_before = 0
        for start in xrange(len(tokens)):
            region = tokens[start:start + max_tokens]
            result = self._get_date(region)
            if result is not None:
                end, date = result
                if allow_overlapping or end > skip_dates_ending_before:
                    skip_dates_ending_before = end
                    yield date

    def generate(self):
        extracts = []
        transforms = []
        aliases = {}
        props_conf = configparser.ConfigParser()
        props_conf.read(os.path.dirname(__file__) + '/../default/props.conf')
        transforms_conf = configparser.ConfigParser()
        transforms_conf.read(os.path.dirname(__file__) + '/../default/transforms.conf')
        if self.sourcetype in props_conf:
            for key, value in props_conf[self.sourcetype].items():
                if key.startswith('extract-'):
                    extracts.append(re.compile(value.replace('?<', '?P<')))
                if key.startswith('report-'):
                    if value in transforms_conf:
                        delim = transforms_conf[value]['DELIMS'].replace('"', '')
                        fields = transforms_conf[value]['FIELDS'].replace('"', '').split(',')
                        transform = {}
                        transform['delim'] = delim
                        transform['fields'] = fields
                        transforms.append(transform)
                if key.startswith('fieldalias-'):
                    match = self.re_alias.match(value)
                    if match:
                        field, alias = match.groups()
                        #print("%s = %s" % (field, alias))
                        aliases[field] = alias
        with FileReadBackwards(self.file) as f:
            i = -1
            for line in f:
                if self.grep:
                    grep = re.compile('.*' + self.grep + '.*')
                    if not grep.match(line):
                        continue
                i = i + 1
                if i < (self.page * self.psize):
                    continue
                if i >= ((self.page + 1) * self.psize):
                    break
                ret = {}
                try:
                    for date in self.find_dates(line, allow_overlapping=False):
                        try:
                            ret['_time'] = date.strftime("%s.%f")
                            break
                        except Exception as e:
                            break
                    if not '_time' in ret:
                        ret['_time'] = time.time()
                    ret['_raw'] = line
                    ret['source'] = self.file
                    ret['sourcetype'] = self.sourcetype
                    for (field, value) in self.kv.findall(line):
                        ret[field] = value.replace('"', '')
                    for extract in extracts:
                        match = extract.search(line)
                        if match:
                            for field, value in match.groupdict().items():
                                ret[field] = value
                    for transform in transforms:
                        f = 0
                        for value in (list(reader([line], delimiter=str(transform['delim'])))[0]):
                            if f >= len(transform['fields']):
                                break
                            if transform['fields'][f] != '':
                                ret[transform['fields'][f]] = value
                            f = f + 1
                    for field, value in ret.items():
                        if field in aliases:
                            ret[aliases[field]] = ret[field]
                    yield ret
                except Exception as e:
                    print("Error: %s." % str(e))
                    pass

dispatch(AdhfileCommand, sys.argv, sys.stdin, sys.stdout, __name__)
