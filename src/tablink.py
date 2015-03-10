#!/usr/bin/python2
"""
Convert Excel files with matching style into RDF cubes

Code derived from TabLinker
"""
import re
import logging
import datetime
import bz2
import os.path
import pprint

from rdflib import ConjunctiveGraph, Literal, RDF, URIRef
from rdflib.namespace import RDFS, XSD
from odf.opendocument import load
from odf.table import Table, TableRow
from odf.namespaces import TABLENS, STYLENS
from odf import text, office, dc
from odf.style import Style
from common.configuration import Configuration
from common import util

import sys
reload(sys)
import traceback
sys.setdefaultencoding("utf8")  # @UndefinedVariable

pp = pprint.PrettyPrinter(indent=2)

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


class TabLink(object):
    def __init__(self, conf, excelFileName, dataFileName, processAnnotations=False):
        """
        Constructor
        """
        self.conf = conf
        self.log = conf.getLogger("TabLink")
        self.excelFileName = excelFileName
        self.dataFileName = dataFileName
        self.processAnnotations = processAnnotations
         
        self.log.debug('Create graph')
        self.graph = ConjunctiveGraph()
        self.conf.bindNamespaces(self.graph)
        
        self.basename = os.path.basename(excelFileName)
        self.basename = re.search('(.*)\.ods', self.basename).group(1)
        
        self.log.debug('Loading Excel file {0}'.format(excelFileName))
        self.book = load(unicode(excelFileName))
        
        self.log.debug('Loading custom styles')
        self.stylesnames = {}
        for style in self.book.getElementsByType(Style):
            parentname = style.getAttrNS(STYLENS, 'parent-style-name')
            name = style.getAttrNS(STYLENS, 'name')
            if parentname != None:
                self.stylesnames[name] = parentname
            
    def doLink(self):
        """
        Start processing all the sheets in workbook
        """
        self.log.debug('Starting TabLink for all sheets in workbook')
        # keep the starting time (ex "2012-04-15T13:00:00-04:00")
        startTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=self.conf.getURI('xsd', 'dateTime'))

        sheets = self.book.getElementsByType(Table)
        
        # Process all the sheets
        self.log.info(self.basename + ':Found %d sheets to process' % len(sheets))
        sheetURIs = []
        for n in range(len(sheets)) :
            self.log.debug('Processing sheet {0}'.format(n))
            try:
                (sheetURI, marked_count) = self.parseSheet(n, sheets[n])
                if marked_count != 0:
                    # Describe the sheet
                    self.graph.add((sheetURI, RDF.type, self.conf.getURI('tablink', 'Sheet')))
                    self.graph.add((sheetURI, RDFS.label, Literal(sheets[n].getAttrNS(TABLENS, 'name'))))
                    # Add it to the dataset
                    sheetURIs.append(sheetURI)
            except Exception as detail:
                self.log.error("Error processing sheet %d of %s" % (n, self.basename))
                self.log.error(sys.exc_info()[0])
                self.log.error(detail)
            
        # end time for the conversion process
        endTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                          datatype=self.conf.getURI('xsd', 'dateTime'))
        
        # Mint the URIs
        datasetURI = self.conf.getURI('cedar', "{0}".format(self.basename))
        distURI = self.conf.getURI('cedar', "{0}-dist".format(self.basename))
        activityURI = self.conf.getURI('cedar', "{0}-tablink".format(self.basename))
        srcURI = self.conf.getURI('cedar', "{0}-src".format(self.basename))
        srcdistURI = self.conf.getURI('cedar', "{0}-src-dist".format(self.basename))
        root = URIRef("https://raw.githubusercontent.com/CEDAR-project/DataDump/master/")
        datasetDumpURI = root + os.path.relpath(self.dataFileName)
        if self.conf.isCompress():
            datasetDumpURI = datasetDumpURI + '.bz2' 
        excelFileURI = root + self.excelFileName
        
        # Describe the data set
        self.graph.add((datasetURI, RDF.type, self.conf.getURI('dcat', 'DataSet')))
        self.graph.add((datasetURI, RDFS.label, Literal(self.basename)))
        self.graph.add((datasetURI, self.conf.getURI('prov', 'wasDerivedFrom'), srcURI))
        self.graph.add((datasetURI, self.conf.getURI('prov', 'wasGeneratedBy'), activityURI))
        for sheetURI in sheetURIs:
            self.graph.add((datasetURI, self.conf.getURI('dcterms', 'hasPart'), sheetURI))
        self.graph.add((datasetURI, self.conf.getURI('dcat', 'distribution'), distURI))
        
        # Describe the distribution of the dataset
        self.graph.add((distURI, RDF.type, self.conf.getURI('dcat', 'Distribution')))
        dumpname = os.path.basename(self.dataFileName)
        if self.conf.isCompress():
            dumpname = dumpname + '.bz2'
        self.graph.add((distURI, RDFS.label, Literal(dumpname)))
        self.graph.add((distURI, self.conf.getURI('dcterms', 'accessURL'), datasetDumpURI))
        
        # Describe the source of the dataset
        self.graph.add((srcURI, RDF.type, self.conf.getURI('dcat', 'DataSet')))
        self.graph.add((srcURI, RDFS.label, Literal(os.path.basename(self.excelFileName))))
        self.graph.add((srcURI, self.conf.getURI('dcat', 'distribution'), srcdistURI))
        self.graph.add((srcURI, self.conf.getURI('tablink', 'sheets'), Literal(len(sheets))))
        
        # Describe the distribution of the source of the dataset
        self.graph.add((srcdistURI, RDF.type, self.conf.getURI('dcat', 'Distribution')))
        self.graph.add((srcdistURI, RDFS.label, Literal(os.path.basename(self.excelFileName))))
        self.graph.add((srcdistURI, self.conf.getURI('dcterms', 'accessURL'), excelFileURI))
        
        # The activity is the conversion process
        self.graph.add((activityURI, RDF.type, self.conf.getURI('prov', 'Activity')))
        self.graph.add((activityURI, self.conf.getURI('prov', 'startedAtTime'), startTime))
        self.graph.add((activityURI, self.conf.getURI('prov', 'endedAtTime'), endTime))
        self.graph.add((activityURI, self.conf.getURI('prov', 'wasAssociatedWith'), self.conf.getURI('tablink', "tabLink")))
        self.graph.add((activityURI, self.conf.getURI('prov', 'used'), srcURI))
        
        # Save the graph
        self.log.info("Saving {} data triples.".format(len(self.graph)))
        try :
            out = bz2.BZ2File(self.dataFileName + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(self.dataFileName, "w")
            self.graph.serialize(destination=out, format='n3')
            out.close()
        except :
            logging.error(self.basename + "Whoops! Something went wrong in serialising to output file")
            logging.info(sys.exc_info())
            traceback.print_exc(file=sys.stdout)
        
    def parseSheet(self, n, sheet):
        """
        Parses the currently selected sheet in the workbook, takes no arguments. Iterates over all cells in the Excel sheet and produces relevant RDF Triples. 
        """        
        # Define a sheetURI for the current sheet
        sheetURI = self.conf.getURI('cedar', "{0}-S{1}".format(self.basename, n))        
        
        columnDimensions = {}
        rowDimensions = {}
        rowProperties = {}
        marked_count = 0
        
        rows = sheet.getElementsByType(TableRow)
        for rowIndex in range(0, len(rows)):
            cols = getColumns(rows[rowIndex])
            for colIndex in range(0, len(cols)):
                cell_obj = cols[colIndex]
                
                if cell_obj == None:
                    continue
                        
                # Get the cell name and the current style
                cellName = colName(colIndex) + str(rowIndex + 1)
                
                if len(cell_obj.getElementsByType(text.P)) == 0:
                    literal = ''
                else:
                    literal = getText(cell_obj)
                    if type(literal) == type(1.0):
                        if literal.is_integer():
                            literal = str(int(literal))
                        else:
                            literal = str(float(literal))
                
                cell = {
                    # Coordinates
                    'i' : rowIndex,
                    'j' : colIndex,
                    # The cell itself
                    'cell' : cell_obj,
                    # The sheet
                    'sheet' : sheet,
                    # The name of the cell
                    'name' : cellName,
                    # The type of the cell
                    'type' : self.getStyle(cell_obj),
                    # The (cleaned) value of the cell
                    'value' : str(literal),
                    # Is empty ?
                    'isEmpty' : str(literal) == '',
                    # Compose a resource name for the cell
                    'URI' : URIRef("{0}-{1}".format(sheetURI, cellName)),
                    # Pass on the URI of the data set
                    'sheetURI' : sheetURI
                }
                
                # self.log.debug("({},{}) {}/{}: \"{}\"". format(i, j, cellType, cellName, cellValue))

                # Increase the counter of marked cells
                if cell['type'] in ['TL Data', 'TL RowHeader', 'TL HRowHeader', 'TL ColHeader', 'TL RowProperty']:
                    marked_count = marked_count + 1
                    
                # Parse cell content
                if cell['type'] == 'TL Data':
                    self.handleData(cell, columnDimensions, rowDimensions)
                elif cell['type'] == 'TL RowHeader' :
                    self.handleRowHeader(cell, rowDimensions, rowProperties)
                elif cell['type'] == 'TL HRowHeader' :
                    self.handleHRowHeader(cell, rowDimensions, rowProperties)
                elif cell['type'] == 'TL ColHeader' :
                    self.handleColHeader(cell, columnDimensions)
                elif cell['type'] == 'TL RowProperty' :
                    self.handleRowProperty(cell, rowProperties)
                elif cell['type'] == 'TL Title' :
                    self.handleTitle(cell)

                # Parse annotation if any and if their processing is enabled
                annotations = cell_obj.getElementsByType(office.Annotation)
                if len(annotations) != 0:
                    self.handleAnnotation(cell, annotations[0])
                
        # Relate all the row properties to their row headers
        for rowDimension in rowDimensions:
                for (p, vs) in rowDimensions[rowDimension].iteritems():
                    for v in vs:
                        self.graph.add((v, self.conf.getURI('tablink', 'parentCell'), p))
        
        # Add additional information about the hierarchy of column headers
        # for value in columnDimensions.values():
        #    for index in range(1, len(value)):
        #        uri_sub = self.getColHeaderValueURI(value[:index + 1])
        #        uri_top = self.getColHeaderValueURI(value[:index])
        #        self.graph.add((uri_sub, self.namespaces['tablink']['subColHeaderOf'], uri_top))
        
        return (sheetURI, marked_count)
        
    def getStyle(self, cell):
        stylename = cell.getAttrNS(TABLENS, 'style-name')
        if stylename != None:
            if stylename.startswith('ce'):
                stylename = self.stylesnames[stylename]
            stylename = stylename.replace('_20_', ' ') 
        return stylename
    
    def handleData(self, cell, columnDimensions, rowDimensions) :
        """
        Create relevant triples for the cell marked as Data
        """
        if cell['isEmpty']:
            return
        
        # Add the cell to the graph
        self._createCell(cell, self.conf.getURI('tablink', 'DataCell'))
            
        # Bind all the row dimensions
        try :
            for dims in rowDimensions[cell['i']].itervalues():
                for dim in dims:
                    self.graph.add((cell['URI'], self.conf.getURI('tablink', 'dimension'), dim))
        except KeyError :
            self.log.debug("({},{}) No row dimension for cell".format(cell['i'], cell['j']))
        
        # Bind all the column dimensions
        try :
            for dim in columnDimensions[cell['j']]:
                self.graph.add((cell['URI'], self.conf.getURI('tablink', 'dimension'), dim))
        except KeyError :
            self.log.debug("({},{}) No column dimension for cell".format(cell['i'], cell['j']))
        
    def handleRowHeader(self, cell, rowDimensions, rowProperties) :
        """
        Create relevant triples for the cell marked as RowHeader
        """
        if cell['isEmpty']:
            return

        # Add the cell to the graph
        self._createCell(cell, self.conf.getURI('tablink', 'RowHeader'))
        
        # Get the row        
        i = cell['i']
        # Get the property for the column
        j = cell['j']
        prop = rowProperties[j]

        rowDimensions.setdefault(i, {})
        rowDimensions[i].setdefault(prop, [])
        rowDimensions[i][prop].append(cell['URI'])
 
    
    def handleHRowHeader(self, cell, rowDimensions, rowProperties) :
        """
        Build up lists for hierarchical row headers. 
        Cells marked as hierarchical row header are often empty meaning 
        that their intended value is stored somewhere else in the Excel sheet.
        """
        # Get the row        
        i = cell['i']
        # Get the property for the column
        j = cell['j']
        prop = rowProperties[j]
        
        if (cell['isEmpty'] or cell['value'].lower() == 'id.' or cell['value'].lower() == 'id ') :
            # If the cell is empty, and a HierarchicalRowHeader, add the value of the row header above it.
            # If the cell is exactly 'id.', add the value of the row header above it.
            try:
                rowDimensions.setdefault(i, {})
                rowDimensions[i].setdefault(prop, [])
                rowDimensions[i][prop].append(rowDimensions[i - 1][prop][0])
            except:
                pass
            # self.log.debug("({},{}) Copied from above\nRow hierarchy: {}".format(i, j, rowValues[i]))
        elif not cell['isEmpty']:
            # Add the cell to the graph
            self._createCell(cell, self.conf.getURI('tablink', 'RowHeader'))
            rowDimensions.setdefault(i, {})
            rowDimensions[i].setdefault(prop, [])
            rowDimensions[i][prop].append(cell['URI'])
            # self.log.debug("({},{}) Added value\nRow hierarchy {}".format(i, j, rowValues[i]))

        # Look if we cover other cells verticaly 
        rows_spanned = cell['cell'].getAttrNS(TABLENS, 'number-rows-spanned')
        if rows_spanned != None:
            rows_spanned = int(rows_spanned)
            for extra in range(1, rows_spanned):
                spanned_row = cell['i'] + extra
                self.log.debug("Span over ({},{})".format(spanned_row, cell['j']))
                rowDimensions.setdefault(spanned_row, {})
                rowDimensions[spanned_row].setdefault(prop, [])
                rowDimensions[spanned_row][prop].append(cell['URI'])
    
    def handleColHeader(self, cell, columnDimensions) :
        """
        Create relevant triples for the cell marked as Header
        """
        # Add the cell to the graph
        self.log.debug("({},{}) Add column dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
        self._createCell(cell, self.conf.getURI('tablink', 'ColumnHeader'))
        # If there is already a parent dimension, connect to it
        if cell['j'] in columnDimensions:
            self.graph.add((cell['URI'], self.conf.getURI('tablink', 'parentCell'), columnDimensions[cell['j']][-1]))    
        dimension = cell['URI']
            
        # Add the dimension to the dimensions list for that column
        columnDimensions.setdefault(cell['j'], []).append(dimension)
        
        # Look if we cover other cells
        columns_spanned = cell['cell'].getAttrNS(TABLENS, 'number-columns-spanned')
        if columns_spanned != None:
            columns_spanned = int(columns_spanned)
            for extra in range(1, columns_spanned):
                spanned_col = cell['j'] + extra
                self.log.debug("Span over ({},{})".format(cell['i'], spanned_col))
                columnDimensions.setdefault(spanned_col, []).append(dimension)
        
        
    def handleRowProperty(self, cell, rowProperties) :
        """
        Create relevant triples for the cell marked as Property Dimension
        """
        
        # Add the cell to the graph
        self.log.debug("({},{}) Add property dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
        self._createCell(cell, self.conf.getURI('tablink', 'RowProperty'))
        rowProperties[cell['j']] = cell['URI']        
        
        # Look if we cover other cells
        columns_spanned = cell['cell'].getAttrNS(TABLENS, 'number-columns-spanned')
        if columns_spanned != None:
            columns_spanned = int(columns_spanned)
            for extra in range(1, columns_spanned):
                self.log.debug("Span over ({},{})".format(cell['i'], cell['j'] + extra))
                rowProperties[cell['j'] + extra] = cell['URI']
            
    
    def handleTitle(self, cell) :
        """
        Create relevant triples for the cell marked as Title 
        """
        self.graph.add((cell['sheetURI'], RDFS.comment, Literal(util.clean_string(cell['value']))))        
    
    def handleAnnotation(self, cell, annotation) :
        """
        Create relevant triples for the annotation attached to the cell
        """
        
        # Create triples according to Open Annotation model
        annotation_URI = cell['URI'] + "-oa"
        annotation_body_URI = annotation_URI + '-body'

        self.graph.add((annotation_URI, RDF.type, self.conf.getURI('oa', 'Annotation')))
        self.graph.add((annotation_URI, self.conf.getURI('oa', 'hasTarget'), cell['URI']))
        self.graph.add((annotation_URI, self.conf.getURI('oa', 'hasBody'), annotation_body_URI))
        
        self.graph.add((annotation_body_URI, RDF.type, RDFS.Resource))
        self.graph.add((annotation_body_URI, self.conf.getURI('tablink', 'value'), Literal(util.clean_string(getText(annotation)))))
        # Extract author
        author = annotation.getElementsByType(dc.Creator)
        if len(author) > 0:
            author = util.clean_string(str(author[0]))
            self.graph.add((annotation_body_URI, self.conf.getURI('oa', 'annotatedBy'), Literal(author)))
        # Extract date
        creation_date = annotation.getElementsByType(dc.Date)
        if len(creation_date) > 0:
            creation_date = str(creation_date[0])
            self.graph.add((annotation_body_URI, self.conf.getURI('oa', 'serializedAt'), Literal(creation_date, datatype=XSD.date)))
            
    # ##
    #    Utility Functions
    # ## 
    def _createCell(self, cell, cell_type):
        """
        Create a new cell
        """
        
        # Set the value
        value = Literal(util.clean_string(cell['value']))
            
        # It's a cell
        self.graph.add((cell['URI'], RDF.type, cell_type))
        
        # It's in the data set defined by the current sheet
        self.graph.add((cell['URI'], self.conf.getURI('tablink', 'sheet'), cell['sheetURI']))
        
        # Add its value (removed the datatype=XSD.decimal because we can't be sure)
        self.graph.add((cell['URI'], self.conf.getURI('tablink', 'value'), value))
        
        # Add a cell label
        label = "Cell %s=%s" % (cell['name'], cell['value'])
        self.graph.add((cell['URI'], RDFS.label, Literal(label)))
        
if __name__ == '__main__':
    config = Configuration('config.ini')
    
    # Test
    inputFile = "data-test/simple.ods"
    #inputFile = "data-test/VT_1899_07_H1.ods"
    dataFile = "/tmp/data.ttl"

    tLinker = TabLink(config, inputFile, dataFile, processAnnotations=True)
    tLinker.doLink()
    

        
