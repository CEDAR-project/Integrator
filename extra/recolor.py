#!/usr/bin/python2
# -*- coding: utf-8 -*-
import sys
import os
import glob
import copy
import logging
from odf.opendocument import OpenDocumentSpreadsheet, load
from odf.style import Style, TableCellProperties, TableColumnProperties, ParagraphProperties
from odf.table import Table, TableColumn, TableRow, TableCell
from odf.element import Element
from odf.namespaces import TABLENS, STYLENS, FONS

# List of styles for TabLink
# http://www.color-hex.com/color-palette/1955

tl_styles = {'TL ColHeader':'#69D2E7',
             'TL RowHeader':'#FAE600',
             'TL HRowHeader':'#A7DBD8',
             'TL Metadata':'#E0E4CC',
             'TL RowLabel':'#F38630',
             'TL RowProperty':'#FA6900',
             'TL Title':'#E77E69',
             'TL Data':'#C8C8A9'
             }

logFormat = '%(asctime)s %(name)-7s %(levelname)-8s %(message)s'
logging.basicConfig(format = logFormat)
logger = logging.getLogger('recolor')
logger.setLevel(logging.INFO)

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
                columns.append(n)
                repeat = repeat - 1
        
        # Move to next node
        node = node.nextSibling
    return columns

def copyStyle(style, autoStylesCache):
    autoStylesCache['lastIndex'] = autoStylesCache['lastIndex'] + 1
    cloneName = 'ce%d' % autoStylesCache['lastIndex']
    logger.debug('copy style : %s -> %s' % (style.getAttrNS(STYLENS, 'name'), cloneName))
    
    # Create the clone
    cloneStyle = Style(name=cloneName,family="table-cell")
    for node in style.childNodes:
        newNode = copy.copy(node)
        newNode.parentNode = None
        cloneStyle.appendChild(newNode)
    
    # Save the clone into the cache
    autoStylesCache['root'].addElement(cloneStyle)
    autoStylesCache['styles'][cloneName] = cloneStyle
    
    return cloneStyle

def setColor(cell, autoStylesCache, color):
    '''
    Assign a new color to a cell. This will modify the parent style of the cell
    and can have an impact on other cells in the document. There are different
    cases covered one by one
    '''    
    currentStyleName = cell.getAttrNS(TABLENS, 'style-name')
    logger.debug('Set style color: %s -> %s' % (currentStyleName, color))

    # The name of the new style
    newStyleName = None
    
    if currentStyleName in autoStylesCache['styles']:
        # Get the current style name
        currentStyle = autoStylesCache['styles'][currentStyleName]
    
        # See if we already have a mapping color
        if color in autoStylesCache['mapping'][currentStyleName]:
            # There is a different style to use. Apply it and return
            newStyleName = autoStylesCache['mapping'][currentStyleName][color]
        else:
            if len(autoStylesCache['mapping'][currentStyleName]) != 0:
                # There is at least one mapping already.
                # Clone the style and remove the (eventual) previous color
                cloneStyle = copyStyle(currentStyle, autoStylesCache)
                cloneStyleName = cloneStyle.getAttrNS(STYLENS, 'name')
                cloneStyle.setAttrNS(STYLENS, 'parent-style-name', 'Default')
                # Switch to the clone
                currentStyle = cloneStyle
                newStyleName = cloneStyleName
            else:
                # First time we color this style
                newStyleName = currentStyleName
            # Set the color
            currentStyle.setAttrNS(STYLENS, 'parent-style-name', color)
            # Save the mapping
            autoStylesCache['mapping'][currentStyleName][color] = newStyleName

    else:
        # The style has no style, just apply the color directly
        newStyleName = color
        
    # Apply the style            
    logger.debug('Set cell style: %s' % newStyleName)
    cell.setAttrNS(TABLENS, 'style-name', newStyleName)
        
       
def recolor(fileName, markingFileName, outputFileName):
    # Load the document
    logger.info("Load %s" % fileName)
    doc = load(fileName)
    
    # Create the styles
    style = Style(name="TabLinker", family="table-cell", parentstylename="Default")
    doc.styles.addElement(style)
    for (style_name, style_color) in tl_styles.iteritems():
        style = Style(name=style_name, family="table-cell", parentstylename="TabLinker")
        style.addElement(TableCellProperties(backgroundcolor=style_color))
        doc.styles.addElement(style)

    # Import color marking if it exists
    if os.path.exists(markingFileName):
        logger.info('Load {0}'.format(markingFileName))
        marking = {}
        for mrk in open(markingFileName):
                (index_str, cell, style) = mrk.strip().split(';')
                index = int(index_str)
                marking.setdefault(index, {})
                marking[index][cell] = style
                
        logger.info('Apply colors')
        
        # Make a cache of the automatic styles
        autoStylesCache = {}
        # Replacement styles for those use for different markings
        autoStylesCache['mapping'] = {}
        # The styles
        autoStylesCache['styles'] = {}
        # Last index of automated style name
        autoStylesCache['lastIndex'] = 0
        # The root in the document
        autoStylesCache['root'] = doc.getElementsByType(Style)[0].parentNode
        for st in doc.getElementsByType(Style):
            styleName = st.getAttrNS(STYLENS, 'name')
            autoStylesCache['styles'][styleName] = st
            autoStylesCache['mapping'][styleName] = {}
            if styleName.startswith('ce'):
                index = int(styleName.replace('ce',''))
                if index > autoStylesCache['lastIndex']:
                    autoStylesCache['lastIndex'] = index
        
        # Start coloring
        tables = doc.getElementsByType(Table)
        for tableIndex in range(0, len(tables)):
            if tableIndex in marking:
                coloredCells = set()
                table = tables[tableIndex]
                rows = table.getElementsByType(TableRow)
                for rowIndex in range(0, len(rows)):
                    cols = getColumns(rows[rowIndex])
                    for colIndex in range(0, len(cols)):
                        cell = cols[colIndex]
                        
                        # Ignore cells that are spanned over
                        if cell == None or cell in coloredCells:
                            continue
                
                        # Get the cell name and the current style
                        cellName = colName(colIndex) + str(rowIndex + 1)
                        
                        # Debug
                        #if cellName in ['A1', 'B3', 'G3','J8']:
                        #    logger.setLevel(logging.DEBUG)
                        #else:
                        #    logger.setLevel(logging.INFO)
                            
                        # Get the color to assign to the cell, if any
                        if cellName not in marking[tableIndex]:
                            color = 'Default' # use default as parent style
                        else:
                            color = marking[tableIndex][cellName]
                            
                        # Change the color
                        logger.debug("--- %s ---" % cellName)
                        setColor(cell, autoStylesCache, color)
    
                        # Add the cell to the set of colored ones
                        # (rows can contain copies of cells if they span)
                        coloredCells.add(cell)
                        
        # Suppress the background colors
        # Suppress the other color if any
        for (cellStyleName,cellStyle) in autoStylesCache['styles'].iteritems():
            if cellStyleName.startswith('ce'):
                for current in cellStyle.getElementsByType(TableCellProperties):
                    currentVal = current.getAttrNS(FONS, 'background-color')
                    if current != None and currentVal != None:
                        current.removeAttrNS(FONS, 'background-color')
    
    # Save                            
    logger.info('Save {0}'.format(outputFileName))
    doc.save(outputFileName)

    
if __name__ == "__main__":
    # Run one
    #recolor(unicode(sys.argv[1]), unicode(sys.argv[2]), unicode(sys.argv[3]))
    
    # Process everything
    for inputFileName in sorted(glob.glob('/tmp/census/*.ods')):
        markingFileName = inputFileName.replace('.ods', '.txt')
        outFileNameODS = inputFileName.replace('census','new')
        if not os.path.exists(outFileNameODS):
            recolor(unicode(inputFileName), unicode(markingFileName), unicode(outFileNameODS))

# Test cases
# BRT_1889_08_T4 -> Bug
# BRT_1899_06_T -> huge

