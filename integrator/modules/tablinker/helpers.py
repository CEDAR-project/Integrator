import re

from odf import text, office, dc
from odf.namespaces import TABLENS, STYLENS

def getColumns(row):
    columns = []
    node = row.firstChild
    end = row.lastChild
    while node != end:
        (_, t) = node.qname
        
        # Focus on (covered) table cells only
        if t != 'covered-table-cell' and t != 'table-cell':
            continue
        
        # If the cell is covered insert a None, otherwise use the cell
        n = node if t == 'table-cell' else None
        columns.append(n)
        
        # Shall we repeat this ?
        repeat = node.getAttrNS(TABLENS, 'number-columns-repeated')
        if repeat != None:
            repeat = int(repeat) - 1
            while repeat != 0:
                columns.append(n)
                repeat = repeat - 1
        
        # Move to next node
        node = node.nextSibling
    return columns

def colName(number):
    ordA = ord('A')
    length = ord('Z') - ordA + 1
    output = ""
    while (number >= 0):
        output = chr(number % length + ordA) + output
        number = number // length - 1
    return output


def getText(cell_obj):
    val = []
    for c in cell_obj.childNodes:
        if c.isInstanceOf(text.P):
            val.append(str(c))
    return ' '.join(val)

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
