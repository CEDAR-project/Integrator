#!/usr/bin/env python
# -*- coding: utf-8 -*-
from odf.opendocument import OpenDocumentSpreadsheet
from odf.style import Style, TextProperties, ParagraphProperties
from odf.style import TableColumnProperties
from odf.text import P
from odf.table import Table, TableColumn, TableRow, TableCell

doc = OpenDocumentSpreadsheet()
# Create a style for the table content. One we can modify
# later in the word processor.
tablecontents = Style(name="Table Contents", family="table-cell", parentstylename="Default")
tablecontents.addElement(ParagraphProperties(numberlines="false", linenumber="0"))
doc.styles.addElement(tablecontents)

# Create automatic styles for the column widths.
# We want two different widths, one in inches, the other one in metric.
# ODF Standard section 15.9.1
widthshort = Style(name="Wshort", family="table-column")
widthshort.addElement(TableColumnProperties(columnwidth="1.7cm"))
doc.automaticstyles.addElement(widthshort)

widthwide = Style(name="Wwide", family="table-column")
widthwide.addElement(TableColumnProperties(columnwidth="1.5in"))
doc.automaticstyles.addElement(widthwide)

# Start the table, and describe the columns
table = Table(name="Password")
table.addElement(TableColumn(numbercolumnsrepeated=4,stylename=widthshort))
table.addElement(TableColumn(numbercolumnsrepeated=3,stylename=widthwide))

for line in ['a:b']:
    rec = line.strip().split(":")
    tr = TableRow()
    table.addElement(tr)
    for val in rec:
        tc = TableCell()
        tr.addElement(tc)
        p = P(stylename=tablecontents,text=val)
        tc.addElement(p)

doc.spreadsheet.addElement(table)
print doc
doc.save(u"/tmp/passwd", True)
