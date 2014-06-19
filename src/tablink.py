#!/usr/bin/python
"""
Convert Excel files with matching style into RDF cubes

Code derived from TabLinker
"""
from xlutils.margins import number_of_good_cols, number_of_good_rows
from xlrd import open_workbook, XL_CELL_EMPTY, XL_CELL_BLANK, cellname
from rdflib import ConjunctiveGraph, Literal, RDF, BNode, URIRef, XSD
from urlparse import urlparse
import re
import urllib
import logging
import datetime
import bz2
import os.path

import sys
from common.configuration import Configuration
reload(sys)
import traceback
sys.setdefaultencoding("utf8")  # @UndefinedVariable


class TabLinker(object):
    def __init__(self, conf, excelFileName, markingFileName, dataFileName, annotationsFileName=None):
        """
        Constructor
        """
        self.log = logging.getLogger("TabLinker")
        self.conf = conf
        self.excelFileName = excelFileName
        self.markingFileName = markingFileName
        self.dataFileName = dataFileName
        self.annotationsFileName = annotationsFileName
         
        self.log.debug('Create graphs')
        self.dataGraph = ConjunctiveGraph()
        self.conf.bindNamespaces(self.dataGraph)
        if self.annotationsFileName != None:
            self.annotationGraph = ConjunctiveGraph()
            self.conf.bindNamespaces(self.annotationGraph)
        
        self.basename = os.path.basename(excelFileName)
        self.basename = re.search('(.*)\.xls', self.basename).group(1)
        
        self.log.debug('Loading Excel file {0}'.format(excelFileName))
        self.wb = open_workbook(excelFileName, formatting_info=True)
        
        self.log.debug('Loading Marking file {0}'.format(markingFileName))
        self.marking = {}
        for mrk in open(markingFileName):
            (index_str, cell, style) = mrk.strip().split(';')
            index = int(index_str)
            self.marking.setdefault(index, {})
            self.marking[index][cell] = style

    def doLink(self):
        """
        Start processing all the sheets in workbook
        """
        self.log.info('Starting TabLinker for all sheets in workbook')

        # Process all the sheets        
        for n in range(self.wb.nsheets) :
            self.log.info('Processing sheet {0}'.format(n))
            self.parseSheet(n)
    
        # Save the result
        self.log.info("Saving {} data triples.".format(len(self.dataGraph)))
        try :
            out = bz2.BZ2File(self.dataFileName + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(self.dataFileName, "w")
            self.dataGraph.serialize(destination=out, format='n3')
            # out.writelines(turtle)
            out.close()
                                
            # Annotations
            if self.annotationsFileName != None:
                self.log.info("Saving {} annotation triples.".format(len(self.annotationGraph)))
                out = bz2.BZ2File(self.annotationsFileName + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(self.annotationsFileName, "w")
                turtle = self.annotationGraph.serialize(destination=None, format='n3')
                out.writelines(turtle)
                out.close()
        except :
            logging.error("Whoops! Something went wrong in serializing to output file")
            logging.info(sys.exc_info())
            traceback.print_exc(file=sys.stdout)
            

        self.log.info('Done !')
    # eg:lifeExpectancy  a rdf:Property, qb:MeasureProperty;
    # rdfs:label "life expectancy"@en;
    # rdfs:subPropertyOf sdmx-measure:obsValue;
    # rdfs:range xsd:decimal . 
    
    def parseSheet(self, n):
        """
        Parses the currently selected sheet in the workbook, takes no arguments. Iterates over all cells in the Excel sheet and produces relevant RDF Triples. 
        """
        sheet = self.wb.sheet_by_index(n)
        colns = number_of_good_cols(sheet)
        rowns = number_of_good_rows(sheet)
        self.log.info("Parsing {0} rows and {1} columns in sheet \"{2}\"".format(rowns, colns, sheet.name))
        
        # Define a datasetURI for the current sheet
        datasetURI = self.conf.getURI('cedar', "{0}_S{1}".format(self.basename, n))
        
        columnDimensions = {}
        propertyDimensions = {}
        rowDimensions = {}
        rowHierarchy = {}
        
        for i in range(0, rowns):
            rowHierarchy[i] = {}
            for j in range(0, colns):
                # Skip cells for which no marking information is available
                if cellname(i, j) not in self.marking[n]:
                    continue
                
                # Prepare context for processing the cell
                cell = {
                    # Coordinates
                    'i' : i,
                    'j' : j,
                    # The cell itself
                    'cell' : sheet.cell(i, j),
                    # The sheet
                    'sheet' : sheet,
                    # Column dimensions
                    'columnDimensions' : columnDimensions,
                    # The name of the cell
                    'name' : cellname(i, j),
                    # The type of the cell
                    'type' : self.marking[n][cellname(i, j)],
                    # The (cleaned) value of the cell
                    'value' : str(sheet.cell(i, j).value).strip(),
                    # Is empty ?
                    'isEmpty' : self.isEmpty(sheet.cell(i, j)),
                    # Compose a resource name for the cell
                    'URI' : URIRef("{0}_{1}".format(datasetURI, cellname(i, j))),
                    # Pass on the URI of the dataset
                    'datasetURI' : datasetURI
                }
                
                # self.log.debug("({},{}) {}/{}: \"{}\"". format(i, j, cellType, cellName, cellValue))
                                            
                # Parse annotation if any and if their processing is enabled
                if (i, j) in sheet.cell_note_map and self.annotationsFileName != None:
                    self.parseAnnotation(cell)

                # Parse cell content
                if cell['type'] == 'TL Data':
                    self.handleData(cell)
                elif cell['type'] == 'TL HRowHeader' :
                    self.handleHRowHeader(cell, rowHierarchy)
                elif cell['type'] == 'TL ColHeader' :
                    self.handleColHeader(cell)
                # elif self.cellType == 'RowProperty' :
                #    self.parseRowProperty(i, j)
                # elif self.cellType == 'Title' :
                #    self.parseTitle(i, j)
                # elif self.cellType == 'RowHeader' :
                #    self.parseRowHeader(i, j)
                # elif self.cellType == 'HRowHeader' :
                #    self.parseHierarchicalRowHeader(i, j)
                # elif self.cellType == 'RowLabel' :
                #    self.parseRowLabel(i, j)
        
        # Add additional information about the hierarchy of column headers
        # for value in columnDimensions.values():
        #    for index in range(1, len(value)):
        #        uri_sub = self.getColHeaderValueURI(value[:index + 1])
        #        uri_top = self.getColHeaderValueURI(value[:index])
        #        self.graph.add((uri_sub, self.namespaces['tablink']['subColHeaderOf'], uri_top))
        
        self.log.info("Done parsing...")
   
    def handleData(self, cell) :
        """
        Create relevant triples for the cell marked as Data
        """
        if cell['isEmpty']:
            return
        
        # It's an observation
        self.dataGraph.add((cell['URI'], RDF.type, self.conf.getURI('qb', 'Observation')))
        
        # It's in the data set defined by the current sheet
        self.dataGraph.add((cell['URI'], self.conf.getURI('qb', 'dataSet'), cell['datasetURI']))
        
        # Add it's value
        self.dataGraph.add((cell['URI'], self.conf.getURI('tablink', 'value'), Literal(cell['value'], datatype=XSD.decimal)))
        
        # Use the row dimensions dictionary to find the properties that link
        # data values to row headers
        # try :
        #    for (prop, value) in self.row_dimensions[i].iteritems() :
        #        self.graph.add((observation, prop, value))
        # except KeyError :
        #    self.log.debug("({}.{}) No row dimension for cell".format(i, j))
        
        # Bind all the column dimensions
        try:
            for dim in cell['columnDimensions'][cell['j']]:
                self.dataGraph.add((cell['URI'], self.conf.getURI('tablink', 'dimension'), dim))
        except KeyError:
            self.log.debug("({},{}) No column dimension for the cell".format(cell['i'], cell['j']))
        
    def handleHRowHeader(self, cell, rowHierarchy) :
        """
        Build up lists for hierarchical row headers. 
        Cells marked as hierarchical row header are often empty meaning 
        that their intended value is stored somewhere else in the Excel sheet.
        """
        i = cell['i']
        j = cell['j']
        
        if (cell['isEmpty'] or cell['value'].lower() == 'id.') :
            # If the cell is empty, and a HierarchicalRowHeader, add the value of the row header above it.
            # If the cell above is not in the rowhierarchy, don't do anything.
            # If the cell is exactly 'id.', add the value of the row header above it. 
            try :
                rowHierarchy[i][j] = rowHierarchy[i - 1][j]
                # self.log.debug("({},{}) Copied from above\nRow hierarchy: {}".format(i, j, rowHierarchy[i]))
            except :
                pass
                # REMOVED because of double slashes in uris
                # self.rowhierarchy[i][j] = self.source_cell.value
                # self.log.debug("({},{}) Top row, added nothing\nRow hierarchy: {}".format(i, j, rowHierarchy[i]))
        elif cell['value'].lower().startswith('id.') or cell['value'].lower().startswith('id '):
            # If the cell starts with 'id.', add the value of the row  above it, and append the rest of the cell's value.
            suffix = cell['value'][3:]               
            try :       
                rowHierarchy[i][j] = rowHierarchy[i - 1][j] + suffix
                # self.log.debug("({},{}) Copied from above+suffix\nRow hierarchy {}".format(i, j, rowHierarchy[i]))
            except :
                rowHierarchy[i][j] = cell['value']
                # self.log.debug("({},{}) Top row, added value\nRow hierarchy {}".format(i, j, rowHierarchy[i]))
        elif not cell['isEmpty']:
            rowHierarchy[i][j] = cell['value']
            # self.log.debug("({},{}) Added value\nRow hierarchy {}".format(i, j, rowHierarchy[i]))
    
    def handleColHeader(self, cell) :
        """
        Create relevant triples for the cell marked as Header
        """
        dimension = None
        
        # If inside a merge box get the parent dimension otherwise create
        # a new dimension
        if self.insideMergeBox(cell['sheet'], cell['i'], cell['j']) and cell['isEmpty']:
            k, l = self.getMergeBoxCoord(cell['sheet'], cell['i'], cell['j'])
            self.log.debug("({},{}) Inside merge box ({}, {})".format(cell['i'], cell['j'], k, l))
            dimension = cell['columnDimensions'][l][-1]
        else:
            # Add a new dimension to the graph
            self.log.debug("({},{}) Add column dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
            self.dataGraph.add((cell['URI'], RDF.type, self.conf.getURI('tablink', 'ColumnHeader')))
            self.dataGraph.add((cell['URI'], self.conf.getURI('skos', 'prefLabel'), Literal(cell['value'])))
            self.dataGraph.add((cell['URI'], self.conf.getURI('tablink', 'cell'), Literal(cell['name'])))
            # If there is already a parent dimension, connect to it
            if cell['j'] in cell['columnDimensions']:
                self.dataGraph.add((cell['URI'], self.conf.getURI('tablink', 'parentCell'), cell['columnDimensions'][cell['j']][-1]))
        
            dimension = cell['URI']
            
        # Add the dimension to the dimensions list for that column
        cell['columnDimensions'].setdefault(cell['j'], []).append(dimension)
        
    def parseHierarchicalRowHeader(self, i, j) :
        """
        Create relevant triples for the cell marked as HierarchicalRowHeader (i, j are row and column)
        """
        
        # Use the rowhierarchy to create a unique qname for the cell's contents, 
        # give the source_cell's original value as extra argument
        self.log.debug("Parsing HierarchicalRowHeader")
            
        # Add all the values
        for (index, value) in self.rowhierarchy[i].items():
            prop = self.property_dimensions[index]
            self.row_dimensions.setdefault(i, {})
            self.row_dimensions[i][self.namespaces['scope'][prop]] = Literal(value)
            
        # Relate the hierarchical headers
        keys = self.rowhierarchy[i].keys()
        for i in range(len(keys) - 1):
            prop_top = self.namespaces['scope'][self.property_dimensions[keys[i]]]
            prop_sub = self.namespaces['scope'][self.property_dimensions[keys[i + 1]]]
            self.graph.add((prop_sub, self.namespaces['tablink']['subPropertyOf'], prop_top))
        

    def parseRowLabel(self, i, j):
        """
        Create relevant triples for the cell marked as Label (i, j are row and column)
        """  
        
        self.log.debug("Parsing Row Label")
        
        # Get the QName of the HierarchicalRowHeader cell that this label belongs to, based on the rowhierarchy for this row (i)
        hierarchicalRowHeader_value_qname = self.getQName(self.rowhierarchy[i])
        
        prefLabels = self.graph.objects(self.namespaces['scope'][hierarchicalRowHeader_value_qname], self.namespaces['skos'].prefLabel)
        for label in prefLabels :
            # If the hierarchicalRowHeader QName already has a preferred label, turn it into a skos:altLabel
            self.graph.remove((self.namespaces['scope'][hierarchicalRowHeader_value_qname], self.namespaces['skos'].prefLabel, label))
            self.graph.add((self.namespaces['scope'][hierarchicalRowHeader_value_qname], self.namespaces['skos'].altLabel, label))
            self.log.debug("Turned skos:prefLabel {} for {} into a skos:altLabel".format(label, hierarchicalRowHeader_value_qname))
        
        # Add the value of the label cell as skos:prefLabel to the header cell
        # self.graph.add((self.namespaces['scope'][hierarchicalRowHeader_value_qname], self.namespaces['skos'].prefLabel, Literal(self.source_cell.value, 'nl')))
            
        # Record that this source_cell_qname is the label for the HierarchicalRowHeader cell
        # self.graph.add((self.namespaces['scope'][self.source_cell_qname], self.namespaces['tablink']['isLabel'], self.namespaces['scope'][hierarchicalRowHeader_value_qname]))
    
    def parseRowHeader(self, i, j) :
        """
        Create relevant triples for the cell marked as RowHeader (i, j are row and column)
        """
        rowHeaderValue = ""

        # Don't attach the cell value to the namespace if it's already a URI
        isURI = urlparse(str(self.source_cell.value))
        if isURI.scheme and isURI.netloc:
            rowHeaderValue = URIRef(self.source_cell.value)
        else:
            self.source_cell_value_qname = self.source_cell.value
            rowHeaderValue = Literal(self.source_cell_value_qname)
        
        # Get the properties to use for the row headers
        prop = self.property_dimensions[j]
        self.row_dimensions.setdefault(i, {})
        self.row_dimensions[i][self.namespaces['scope'][prop]] = rowHeaderValue
        
        return
    
    
    def parseRowProperty(self, i, j) :
        """
        Create relevant triples for the cell marked as Property (i, j are row and column)
        """
        if self.isEmpty(i, j):
            if self.insideMergeBox(i, j):
                k, l = self.getMergeBoxCoord(i, j)
                self.source_cell_value_qname = self.addValue(self.r_sheet.cell(k, l).value)
            else:
                return
        else:
            self.source_cell_value_qname = self.addValue(self.source_cell.value)   
        # self.graph.add((self.namespaces['scope'][self.source_cell_qname],self.namespaces['tablink']['isDimensionProperty'],self.namespaces['scope'][self.source_cell_value_qname]))
        # self.graph.add((self.namespaces['scope'][self.source_cell_value_qname],RDF.type,self.namespaces['qb']['DimensionProperty']))
        # self.graph.add((self.namespaces['scope'][self.source_cell_value_qname],RDF.type,RDF['Property']))
        
        # self.property_dimensions.setdefault(j,[]).append(self.source_cell_value_qname)
        self.property_dimensions[j] = self.source_cell_value_qname
        
        # Add to graph
        resource = self.namespaces['scope'][self.property_dimensions[j]]
        self.graph.add((resource, RDF.type, self.namespaces['tablink']['RowProperty']))

        return
    
    def parseTitle(self, i, j) :
        """
        Create relevant triples for the cell marked as Title (i, j are row and column)
        """
        self.graph.add((self.namespaces['scope'][self.sheet_qname],
                        self.namespaces['tablink']['title'],
                        Literal(self.source_cell.value)))        
        return
        
        

    def parseAnnotation(self, i, j) :
        """
        Create relevant triples for the annotation attached to cell (i, j)
        """

        # Create triples according to Open Annotation model

        body = BNode()

        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  RDF.type,
                                  self.annotationNamespaces['oa']['Annotation']
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['hasBody'],
                                  body
                                  ))
        self.annotationGraph.add((body,
                                  RDF.value,
                                  Literal(self.annotations[(i, j)].text.replace("\n", " ").replace("\r", " ").replace("\r\n", " ").encode('utf-8'))
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['hasTarget'],
                                  self.namespaces['scope'][self.source_cell_qname]
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['annotator'],
                                  Literal(self.annotations[(i, j)].author.encode('utf-8'))
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['annotated'],
                                  Literal(datetime.datetime.fromtimestamp(os.path.getmtime(self.filename)).strftime("%Y-%m-%d"), datatype=self.annotationNamespaces['xsd']['date'])
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['generator'],
                                  URIRef("https://github.com/Data2Semantics/TabLinker")
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['generated'],
                                  Literal(datetime.datetime.now().strftime("%Y-%m-%d"), datatype=self.annotationNamespaces['xsd']['date'])
                                  ))
        self.annotationGraph.add((self.annotationNamespaces['scope'][self.source_cell_qname],
                                  self.annotationNamespaces['oa']['modelVersion'],
                                  URIRef("http://www.openannotation.org/spec/core/20120509.html")
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
         
    def getType(self, style):
        """Get type for a given excel style. Style name must be prefixed by 'TL '
    
        Arguments:
        style -- Style (string) to check type for
        
        Returns:
        String -- The type of this field. In case none is found, 'unknown'
        """
        typematch = re.search('TL\s(.*)', style)
        if typematch :
            cellType = typematch.group(1)
        else :
            cellType = 'Unknown'
        return cellType
    
    def isEmpty(self, cell):
        """
        Check whether a cell is empty.
        
        Returns:
        True/False -- depending on whether the cell is empty
        """
        return (cell.ctype == XL_CELL_EMPTY or cell.ctype == XL_CELL_BLANK or cell.value == '')
        
    def isEmptyRow(self, i, colns):
        """
        Determine whether the row 'i' is empty by iterating over all its cells
        
        Arguments:
        i     -- The index of the row to be checked.
        colns -- The number of columns to be checked
        
        Returns:
        true  -- if the row is empty
        false -- if the row is not empty
        """
        for j in range(0, colns) :
            if not self.isEmpty(i, j):
                return False
        return True
    
    def isEmptyColumn(self, j, rowns):
        """
        Determine whether the column 'j' is empty by iterating over all its cells
        
        Arguments:
        j     -- The index of the column to be checked.
        rowns -- The number of rows to be checked
        
        Returns:
        true  -- if the column is empty
        false -- if the column is not empty
        """
        for i in range(0, rowns) :
            if not self.isEmpty(i, j):
                return False
        return True
    
    def getValidRowsCols(self) :
        """
        Determine the number of non-empty rows and columns in the Excel sheet
        
        Returns:
        rowns -- number of rows
        colns -- number of columns
        """
        colns = number_of_good_cols(self.r_sheet)
        rowns = number_of_good_rows(self.r_sheet)
        
        # Check whether the number of good columns and rows are correct
        while self.isEmptyRow(rowns - 1, colns) :
            rowns = rowns - 1 
        while self.isEmptyColumn(colns - 1, rowns) :
            colns = colns - 1
            
        self.log.debug('Number of rows with content:    {0}'.format(rowns))
        self.log.debug('Number of columns with content: {0}'.format(colns))
        return rowns, colns
    
    def getQName(self, names):
        """
        Create a valid QName from a string or dictionary of names
        
        Arguments:
        names -- Either dictionary of names or string of a name.
        
        Returns:
        qname -- a valid QName for the dictionary or string
        """
        
        if type(names) == dict :
            qname = self.sheet_qname
            for k in names :
                qname = qname + '_' + self.processString(names[k])
        else :
            qname = self.sheet_qname + '_' + self.processString(names)
        
        self.log.debug('Minted new QName: {}'.format(qname))
        return qname

    def processString(self, string):
        """
        Remove illegal characters (comma, brackets, etc) from string, and replace it with underscore. Useful for URIs
        
        Arguments:
        string -- The string representing the value of the source cell
        
        Returns:
        processedString -- The processed string
        """
        # TODO accents too
        return urllib.quote(re.sub('\s|\(|\)|,|\.', '_', unicode(string).strip().replace('/', '-')).encode('utf-8', 'ignore'))
         
if __name__ == '__main__':
    config = Configuration('config.ini')
    
    # Test
    inputFile = "data-test/simple.xls"
    markingFile = "data-test/simple-marking.txt"
    dataFile = "/tmp/data.ttl"

    tLinker = TabLinker(config, inputFile, markingFile, dataFile)
    tLinker.doLink()
    

        
