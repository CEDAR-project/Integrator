#!/usr/bin/python2
"""
Convert Excel files with matching style into RDF cubes

Code derived from TabLinker
"""
from xlutils.margins import number_of_good_cols, number_of_good_rows
from xlutils.styles import Styles
from xlrd import open_workbook, XL_CELL_EMPTY, XL_CELL_BLANK, cellname
from rdflib import ConjunctiveGraph, Literal, RDF, URIRef
import re
import logging
import datetime
import bz2
import os.path
import pprint

import sys
from common.configuration import Configuration
from rdflib.namespace import RDFS
reload(sys)
import traceback
sys.setdefaultencoding("utf8")  # @UndefinedVariable

pp = pprint.PrettyPrinter(indent=2)

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
        self.basename = re.search('(.*)\.xls', self.basename).group(1)
        
        self.log.debug('Loading Excel file {0}'.format(excelFileName))
        self.wb = open_workbook(excelFileName, formatting_info=True, on_demand=True)
        
        self.log.debug('Reading styles')
        self.styles = Styles(self.wb)
        
    def doLink(self):
        """
        Start processing all the sheets in workbook
        """
        self.log.debug('Starting TabLink for all sheets in workbook')
        # keep the starting time (ex "2012-04-15T13:00:00-04:00")
        startTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=self.conf.getURI('xsd', 'dateTime'))

        # Process all the sheets
        self.log.info(self.basename + ':Found %d sheets to process' % self.wb.nsheets)
        sheetURIs = []
        for n in range(self.wb.nsheets) :
            self.log.debug('Processing sheet {0}'.format(n))
            try:
                (sheetURI, marked_count) = self.parseSheet(n)
                if marked_count != 0:
                    # Describe the sheet
                    self.graph.add((sheetURI, RDF.type, self.conf.getURI('tablink', 'Sheet')))
                    self.graph.add((sheetURI, RDFS.label, Literal(self.wb.sheet_by_index(n).name)))
                    # Add it to the dataset
                    sheetURIs.append(sheetURI)
            except:
                self.log.error("Error processing sheet %d of %s" % (n, self.basename))
            
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
        self.graph.add((srcURI, self.conf.getURI('tablink', 'sheets'), Literal(self.wb.nsheets)))
        
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
        
    def parseSheet(self, n):
        """
        Parses the currently selected sheet in the workbook, takes no arguments. Iterates over all cells in the Excel sheet and produces relevant RDF Triples. 
        """        
        sheet = self.wb.sheet_by_index(n)
        colns = number_of_good_cols(sheet)
        rowns = number_of_good_rows(sheet)
        self.log.info(self.basename + ":Parsing {0} rows and {1} columns in sheet \"{2}\"".format(rowns, colns, sheet.name))
        
        # Define a sheetURI for the current sheet
        sheetURI = self.conf.getURI('cedar', "{0}-S{1}".format(self.basename, n))        
        
        columnDimensions = {}
        rowDimensions = {}
        rowProperties = {}
        marked_count = 0
                
        for i in range(0, rowns):
            for j in range(0, colns):
                
                # Prepare context for processing the cell
                cell_obj = sheet.cell(i, j)
        
                literal = cell_obj.value
                if type(literal) == type(1.0):
                    if literal.is_integer():
                        literal = str(int(literal))
                    else:
                        literal = str(float(literal))
                        
                cell = {
                    # Coordinates
                    'i' : i,
                    'j' : j,
                    # The cell itself
                    'cell' : cell_obj,
                    # The sheet
                    'sheet' : sheet,
                    # The name of the cell
                    'name' : cellname(i, j),
                    # The type of the cell
                    'type' : self.styles[cell_obj].name,
                    # The (cleaned) value of the cell
                    'value' : literal,
                    # Is empty ?
                    'isEmpty' : self.isEmpty(cell_obj),
                    # Compose a resource name for the cell
                    'URI' : URIRef("{0}-{1}".format(sheetURI, cellname(i, j))),
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
                if (i, j) in sheet.cell_note_map and self.processAnnotations:
                    self.handleAnnotation(cell)
                
        # Relate all the row properties to their row headers
        for rowDimension in rowDimensions:
                for (p, v) in rowDimensions[rowDimension].iteritems():
                    self.graph.add((v, self.conf.getURI('tablink', 'parentCell'), p))
        
        # Add additional information about the hierarchy of column headers
        # for value in columnDimensions.values():
        #    for index in range(1, len(value)):
        #        uri_sub = self.getColHeaderValueURI(value[:index + 1])
        #        uri_top = self.getColHeaderValueURI(value[:index])
        #        self.graph.add((uri_sub, self.namespaces['tablink']['subColHeaderOf'], uri_top))
        
        return (sheetURI, marked_count)
        
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
            for dim in rowDimensions[cell['i']].itervalues():
                self.graph.add((cell['URI'], self.conf.getURI('tablink', 'dimension'), dim))
        except KeyError :
            self.log.debug("({}.{}) No row dimension for cell".format(cell['i'], cell['j']))
        
        # Bind all the column dimensions
        try :
            for dim in columnDimensions[cell['j']]:
                self.graph.add((cell['URI'], self.conf.getURI('tablink', 'dimension'), dim))
        except KeyError :
            self.log.debug("({}.{}) No column dimension for cell".format(cell['i'], cell['j']))
        
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
        rowDimensions[i][prop] = cell['URI']
 
    
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
                rowDimensions[i][prop] = rowDimensions[i - 1][prop]
            except:
                pass
            # self.log.debug("({},{}) Copied from above\nRow hierarchy: {}".format(i, j, rowValues[i]))
        elif not cell['isEmpty']:
            # Add the cell to the graph
            self._createCell(cell, self.conf.getURI('tablink', 'RowHeader'))
            rowDimensions.setdefault(i, {})
            rowDimensions[i][prop] = cell['URI']
            # self.log.debug("({},{}) Added value\nRow hierarchy {}".format(i, j, rowValues[i]))
    
    def handleColHeader(self, cell, columnDimensions) :
        """
        Create relevant triples for the cell marked as Header
        """
        dimension = None
        
        # If inside a merge box get the parent dimension otherwise create
        # a new dimension
        if self.insideMergeBox(cell['sheet'], cell['i'], cell['j']) and cell['isEmpty']:
            k, l = self.getMergeBoxCoord(cell['sheet'], cell['i'], cell['j'])
            self.log.debug("({},{}) Inside merge box ({}, {})".format(cell['i'], cell['j'], k, l))
            dimension = columnDimensions[l][-1]
        else:
            # Add the cell to the graph
            self.log.debug("({},{}) Add column dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
            self._createCell(cell, self.conf.getURI('tablink', 'ColumnHeader'))
            # If there is already a parent dimension, connect to it
            if cell['j'] in columnDimensions:
                self.graph.add((cell['URI'], self.conf.getURI('tablink', 'parentCell'), columnDimensions[cell['j']][-1]))
        
            dimension = cell['URI']
            
        # Add the dimension to the dimensions list for that column
        columnDimensions.setdefault(cell['j'], []).append(dimension)
        
    def handleRowProperty(self, cell, rowProperties) :
        """
        Create relevant triples for the cell marked as Property Dimension
        """
        dimension = None
        
        if self.insideMergeBox(cell['sheet'], cell['i'], cell['j']) and cell['isEmpty']:
            k, l = self.getMergeBoxCoord(cell['sheet'], cell['i'], cell['j'])
            self.log.debug("({},{}) Inside merge box ({}, {})".format(cell['i'], cell['j'], k, l))
            dimension = rowProperties[l]
        else:
            # Add the cell to the graph
            self.log.debug("({},{}) Add property dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
            self._createCell(cell, self.conf.getURI('tablink', 'RowProperty'))
            dimension = cell['URI']
            
        rowProperties[cell['j']] = dimension        
    
    def handleTitle(self, cell) :
        """
        Create relevant triples for the cell marked as Title 
        """
        self.graph.add((cell['sheetURI'], self.conf.getURI('rdfs', 'comment'), Literal(cell['value'])))        
    
    def handleAnnotation(self, cell) :
        """
        Create relevant triples for the annotation attached to cell (i, j)
        """
        i = cell['i']
        j = cell['j']
        annot = cell['sheet'].cell_note_map[(i, j)]
        
        # Create triples according to Open Annotation model
        annotation = cell['URI'] + "-oa"
        body = annotation + '-body'

        self.graph.add((annotation,
                        RDF.type,
                        self.conf.getURI('oa', 'Annotation')
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa', 'hasTarget'),
                        cell['URI']
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa', 'hasBody'),
                        body
                        ))
        self.graph.add((body,
                        RDF.type,
                        RDFS.Resource
                        ))
        self.graph.add((body,
                        self.conf.getURI('tablink', 'value'),
                        Literal(annot.text.encode('utf-8'))
                        ))
        if annot.author.encode('utf-8') != "":
            self.graph.add((annotation,
                            self.conf.getURI('oa', 'annotatedBy'),
                            Literal(annot.author.encode('utf-8'))
                            ))
        self.graph.add((annotation,
                        self.conf.getURI('oa', 'serializedBy'),
                        URIRef("https://github.com/CEDAR-project/Integrator")
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa', 'serializedAt'),
                        Literal(datetime.datetime.now().strftime("%Y-%m-%d"), datatype=self.conf.getURI('xsd', 'date'))
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa', 'modelVersion'),
                        URIRef("http://www.openannotation.org/spec/core/20130208/index.html")
                        ))
        
    # ##
    #    Utility Functions
    # ## 
    
    def insideMergeBox(self, sheet, i, j):
        """
        Check if the specified cell is inside a merge box

        Arguments:
        i -- row
        j -- column

        Returns:
        True/False -- depending on whether the cell is inside a merge box
        """
        merged_cells = sheet.merged_cells
        for crange in merged_cells:
            rlo, rhi, clo, chi = crange
            if i <= rhi - 1 and i >= rlo and j <= chi - 1 and j >= clo:
                return True
        return False
        

    def getMergeBoxCoord(self, sheet, i, j):
        """
        Get the top-left corner cell of the merge box containing the specified cell

        Arguments:
        i -- row
        j -- column

        Returns:
        (k, l) -- Coordinates of the top-left corner of the merge box
        """
        if not self.insideMergeBox(sheet, i, j):
            return (-1, -1)

        merged_cells = sheet.merged_cells
        for crange in merged_cells:
            rlo, rhi, clo, chi = crange
            if i <= rhi - 1 and i >= rlo and j <= chi - 1 and j >= clo:
                return (rlo, clo)            
         
    def isEmpty(self, cell):
        """
        Check whether a cell is empty.
        
        Returns:
        True/False -- depending on whether the cell is empty
        """
        
        return (cell.ctype == XL_CELL_EMPTY or cell.ctype == XL_CELL_BLANK or cell.value == '')
        
    
    def _createCell(self, cell, cell_type):
        """
        Create a new cell
        """
        
        # Set the value
        value = Literal(str(cell['value']))
            
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
    inputFile = "data-test/simple.xls"
    dataFile = "/tmp/data.ttl"

    tLinker = TabLink(config, inputFile, dataFile, processAnnotations=True)
    tLinker.doLink()
    

        
