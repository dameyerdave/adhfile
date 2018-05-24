#!/usr/bin/env python

import sys, time, re
from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from datetime import datetime
import dateutil.parser
from itertools import chain

@Configuration()
class AdhfileCommand(GeneratingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    ##TODO:
    - datetime parser
    - field extraction by config
    - multiline support

    """
    count = Option(require=True, validate=validators.Integer())
    file = Option(require=True)

    kv = re.compile(r"\b(\w+)\s*?=\s*([^=]*)(?=\s+\w+\s*=|$)")

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
        with open(self.file, "r") as f:
            i = 0
            for line in f:
                i = i + 1
                if i > self.count:
                    break
                ret = {}
                try:
                    for date in self.find_dates(line, allow_overlapping=False):
                        ret['_time'] = date.strftime("%s.%f")
                        break
                    if not '_time' in ret:
                        ret['_time'] = time.time()
                    for (field, value) in self.kv.findall(line):
                        ret[field] = value
                    ret['_raw'] = line
                    yield ret
                except Exception as e:
                    print("Error: %s." % str(e))
                    pass

dispatch(AdhfileCommand, sys.argv, sys.stdin, sys.stdout, __name__)
