#!/usr/bin/python2
# -*- coding: utf-8 -*-
import sys
import os
import glob
from odf.opendocument import OpenDocumentSpreadsheet, load
from odf.style import Style, TableCellProperties, TableColumnProperties, ParagraphProperties
from odf.text import P
from odf.table import Table, TableColumn, TableRow, TableCell
from odf.element import Element
from odf.namespaces import TABLENS

# List of styles for TabLink
tl_styles = {'TL ColHeader':'#69D2E7',
             'TL RowHeader':'#69D2E7',
             'TL HRowHeader':'#A7DBD8',
             'TL Metadata':'#E0E4CC',
             'TL RowLabel':'#F38630',
             'TL RowProperty':'#FA6900',
             'TL Title':'#FC9D9A',
             'TL Data':'#C8C8A9'
             }

def colName(number):
    ordA = ord('A')
    length = ord('Z') - ordA + 1
    output = ""
    while (number >= 0):
        output = chr(number % length + ordA) + output
        number = number // length - 1
    return output

def getColumns(row):
    columns = []
    node = row.firstChild
    end = row.lastChild
    while node != end:
        (_, t) = node.qname
        
        # Focus on table cells only
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
                columns.append(node)
                repeat = repeat - 1
        
        # Move to next node
        node = node.nextSibling
    return columns
    
def recolor(fileName, markingFileName, outputFileName):
    # Load the document
    print "Load %s" % fileName
    doc = load(fileName)
    
    # Create the styles
    style = Style(name="TabLinker", family="table-cell", parentstylename="Default")
    doc.styles.addElement(style)
    for (style_name, style_color) in tl_styles.iteritems():
        style = Style(name=style_name, family="table-cell", parentstylename="Default")
        style.addElement(TableCellProperties(backgroundcolor=style_color, wrapoption="wrap", verticalalign="middle"))
        style.addElement(ParagraphProperties(textalign="center"))
        doc.styles.addElement(style)

    # Import color marking if it exists
    if os.path.exists(markingFileName):
        print 'Load {0}'.format(markingFileName)
        marking = {}
        for mrk in open(markingFileName):
                (index_str, cell, style) = mrk.strip().split(';')
                index = int(index_str)
                marking.setdefault(index, {})
                marking[index][cell] = style
        print 'Apply colors'
        tables = doc.getElementsByType(Table)
        for tableIndex in range(0, len(tables)):
            if tableIndex in marking:
                table = tables[tableIndex]
                rows = table.getElementsByType(TableRow)
                for rowIndex in range(0, len(rows)):
                    cols = getColumns(rows[rowIndex])
                    for colIndex in range(0, len(cols)):
                        cell = cols[colIndex]
                        cellName = colName(colIndex) + str(rowIndex + 1)
                        if cell != None:
                            if cellName in marking[tableIndex]:
                                color = marking[tableIndex][cellName]
                                cell.setAttrNS(TABLENS, 'style-name', color)
                            
    # Save
    print 'Save {0}'.format(outputFileName)
    doc.save(outputFileName)

    
if __name__ == "__main__":
    # Run one
    # recolor(unicode(sys.argv[1]), unicode(sys.argv[2]), unicode(sys.argv[3]))
    
    # Process everything
    for inputFileName in sorted(glob.glob('./data/input/odf/*.odf')):
        markingFileName = inputFileName.replace('.odf', '.txt').replace('/odf','/marking')
        outFileNameODF = inputFileName.replace('/odf','/spreadsheets')
        if not os.path.exists(outFileNameODF):
            recolor(unicode(inputFileName), unicode(markingFileName), unicode(outFileNameODF))
