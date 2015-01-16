#!/usr/bin/python2
import bz2
import datetime
import os
import pprint
import csv
import codecs
import re

from rdflib.term import Literal
from xlrd import open_workbook
from xlutils.margins import number_of_good_cols, number_of_good_rows

import sys
reload(sys)
sys.setdefaultencoding("utf8")  # @UndefinedVariable

PP = pprint.PrettyPrinter(indent=2)
CURRENT_TYPES = "DataDump/mapping/Housing_types.xls"
ADDITION = "DataDump/mapping/Ashkan/HousingClassification"

def clean_string(text):
    """
    Utility function to clean a string
    TODO speed this up
    """
    # Lower and remove new lines
    text_clean = text.lower().replace('\n', ' ').replace('\r', ' ')
    # Shrink spaces
    text_clean = re.sub(r'\s+', ' ', text_clean)
    # Remove lead and trailing whitespace
    text_clean = text_clean.strip()
    return text_clean


# Load the current file mappings
wb = open_workbook(CURRENT_TYPES, formatting_info=False, on_demand=True)
sheet = wb.sheet_by_index(0)
colns = number_of_good_cols(sheet)
rowns = number_of_good_rows(sheet)
mappings = {}
for i in range(1, rowns):
    # Get the string
    literal = clean_string(sheet.cell(i, 1).value)
    mappings.setdefault(literal, set())
    print '>' + literal
    
    # Get the values
    for j in range(2, colns):
        value = sheet.cell(i, j).value
        if value != '':
            mappings[literal].add(value)

    
# Load data from Ashkan
add = 0
with open(ADDITION, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    first = True
    for row in spamreader:
        # Skip header and empty lines
        if first:
            first = False
            continue
        if len(row) < 2:
            continue
        
        # Get the string
        literal = clean_string(row[1])
        mappings.setdefault(literal, set())
        print '>' + literal
        
        # Get the values
        for j in range(2, len(row)):
            value = row[j]
            if value != '':
                if value not in mappings[literal]:
                    add = add + 1
                mappings[literal].add(value)


csvfile = codecs.open('/tmp/new.csv', 'wb', 'utf-8')
spamwriter = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
for (k, v) in mappings.iteritems():
    line = []
    line.append('')
    line.append(k)
    for value in v:
        line.append(value)
    
    #line_str = u','.join(line).encode('utf-8').strip()
    
    spamwriter.writerow (line)
csvfile.close()
    
#    print k,v

# PP.pprint(mappings)
