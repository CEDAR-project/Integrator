#!/usr/bin/python2
"""
Convert Excel files with matching style into RDF cubes

Code derived from TabLinker
"""
from xlutils.margins import number_of_good_cols, number_of_good_rows
from xlrd import open_workbook, XL_CELL_EMPTY, XL_CELL_BLANK, cellname
from rdflib import ConjunctiveGraph, Literal, RDF, BNode, URIRef, RDFS
import re
import logging
import datetime
import bz2
import os.path
import pprint

import sys
from common.configuration import Configuration
reload(sys)
import traceback
sys.setdefaultencoding("utf8")  # @UndefinedVariable

pp = pprint.PrettyPrinter(indent=2)

# HOTFIX needed
# - Several sheets from a same file need to be associated to the same source
# - Do not put the sheet name in the source URI (e.g. no "VT_1947_A1_T_S1-src")
# - Add prov:Entity type to all observations
# - Add prov information to say this was generated with tablink
# - Add schema information with qb terms to better filter observations in cubes

class TabLinker(object):
    def __init__(self, conf, excelFileName, markingFileName, dataFileName, processAnnotations = False):
        """
        Constructor
        """
        self.log = logging.getLogger("TabLinker")
        self.conf = conf
        self.excelFileName = excelFileName
        self.markingFileName = markingFileName
        self.dataFileName = dataFileName
        self.processAnnotations = processAnnotations
         
        self.log.debug('Create graph')
        self.graph = ConjunctiveGraph()
        self.conf.bindNamespaces(self.graph)
        
        self.basename = os.path.basename(excelFileName)
        self.basename = re.search('(.*)\.xls', self.basename).group(1)
        
        self.log.debug('Loading Excel file {0}'.format(excelFileName))
        self.wb = open_workbook(excelFileName, formatting_info=True, on_demand=True)
        
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
        # self.log.info('Starting TabLinker for all sheets in workbook')

        # Process all the sheets        
        for n in range(self.wb.nsheets) :
            # self.log.info('Processing sheet {0}'.format(n))
            self.parseSheet(n)
    
        # Save the result
        self.log.info("Saving {} data triples.".format(len(self.graph)))
        try :
            out = bz2.BZ2File(self.dataFileName + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(self.dataFileName, "w")
            self.graph.serialize(destination=out, format='n3')
            # out.writelines(turtle)
            out.close()
        except :
            logging.error("Whoops! Something went wrong in serializing to output file")
            logging.info(sys.exc_info())
            traceback.print_exc(file=sys.stdout)
        
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
        rowDimensions = {}
        rowProperties = {}
        
        for i in range(0, rowns):
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
                    
        # Add additional information about the hierarchy of column headers
        # for value in columnDimensions.values():
        #    for index in range(1, len(value)):
        #        uri_sub = self.getColHeaderValueURI(value[:index + 1])
        #        uri_top = self.getColHeaderValueURI(value[:index])
        #        self.graph.add((uri_sub, self.namespaces['tablink']['subColHeaderOf'], uri_top))
        
        self.log.info("Done parsing...")
        
        # Write the ontology for tablinker
        self.graph.add((self.conf.getURI('tablink', 'value'), RDF.type, self.conf.getURI('qb', 'MeasureProperty')))
        self.graph.add((self.conf.getURI('tablink', 'cell'), RDF.type, RDF.Property))
        self.graph.add((self.conf.getURI('tablink', 'parentCell'), RDF.type, RDF.Property))
        self.graph.add((self.conf.getURI('tablink', 'dimension'), RDF.type, self.conf.getURI('qb', 'DimensionProperty')))
        self.graph.add((self.conf.getURI('tablink', 'ColumnHeader'), RDFS.subClassOf, self.conf.getURI('qb', 'DimensionProperty')))
        
        # Write the DSD
        dsdURI = datasetURI + "-dsd"
        self.graph.add((dsdURI,
                        RDF.type,
                        self.conf.getURI('qb', 'DataStructureDefinition')
                        ))
        ## The dimensions
        order = 1
        dimensions = []
        for (_,dimmap) in rowDimensions.iteritems():
            for (dim, _) in dimmap.iteritems():
                if dim not in dimensions:
                    dimensions.append(dim)
        for dim in dimensions:
            node = BNode();
            self.graph.add((dsdURI,
                            self.conf.getURI('qb', 'component'),
                            node
                            ))
            self.graph.add((node,
                            self.conf.getURI('qb', 'dimension'),
                            dim
                            ))
            self.graph.add((node,
                            self.conf.getURI('qb', 'order'),
                            Literal(order)
                            ))
            order = order + 1
        node = BNode();
        self.graph.add((dsdURI,
                        self.conf.getURI('qb', 'component'),
                        node
                        ))
        self.graph.add((node,
                        self.conf.getURI('qb', 'dimension'),
                        self.conf.getURI('tablink', 'dimension')
                        ))
        self.graph.add((node,
                        self.conf.getURI('qb', 'order'),
                        Literal(order)
                        ))
        order = order + 1
        
        ## The measure
        node = BNode();
        self.graph.add((dsdURI,
                        self.conf.getURI('qb', 'component'),
                        node
                        ))
        self.graph.add((node,
                        self.conf.getURI('qb', 'measure'),
                        self.conf.getURI('tablink', 'value')
                        ))
                        
        # Describe the data set
        self.graph.add((datasetURI,
                        RDF.type,
                        self.conf.getURI('qb', 'DataSet')
                        ))
        self.graph.add((datasetURI,
                        RDF.type,
                        self.conf.getURI('prov', 'Entity')
                        ))
        self.graph.add((datasetURI,
                        self.conf.getURI('qb', 'structure'),
                        dsdURI
                        ))
        self.graph.add((datasetURI,
                        self.conf.getURI('tablink', 'sheetName'),
                        Literal(sheet.name)
                        ))
        srcURI = datasetURI + '-src'
        self.graph.add((datasetURI,
                        self.conf.getURI('prov', 'wasDerivedFrom'),
                        srcURI
                        ))
        self.graph.add((srcURI,
                        RDF.type,
                        self.conf.getURI('dcat', 'DataSet')
                        ))
        fileUri = srcURI + '-file'
        self.graph.add((srcURI,
                        self.conf.getURI('dcat', 'distribution'),
                        fileUri
                        ))
        self.graph.add((fileUri,
                        self.conf.getURI('dcterms', 'title'),
                        Literal(os.path.basename(self.excelFileName))
                        ))
        self.graph.add((fileUri,
                        self.conf.getURI('dcterms', 'accessURL'),
                        URIRef('file://'+os.path.abspath(self.excelFileName))
                        ))
    
    def handleData(self, cell, columnDimensions, rowDimensions) :
        """
        Create relevant triples for the cell marked as Data
        """
        if cell['isEmpty']:
            return
        
        # It's an observation
        self.graph.add((cell['URI'], RDF.type, self.conf.getURI('qb', 'Observation')))
        
        # It's in the data set defined by the current sheet
        self.graph.add((cell['URI'], self.conf.getURI('qb', 'dataSet'), cell['datasetURI']))
        
        # Add it's value (removed the datatype=XSD.decimal because we can't be sure)
        self.graph.add((cell['URI'], self.conf.getURI('tablink', 'value'), Literal(cell['value'])))
        
        # Bind all the row dimensions
        try :
            for (prop, value) in rowDimensions[cell['i']].iteritems() :
                self.graph.add((cell['URI'], prop, Literal(value)))
        except KeyError :
            self.log.debug("({}.{}) No row dimension for cell".format(cell['i'], cell['j']))
        
        # Bind the last of column dimensions, the others are linked via the parent cell property
        dim = columnDimensions[cell['j']][-1]
        self.graph.add((cell['URI'], self.conf.getURI('tablink', 'dimension'), dim))
        
    def handleRowHeader(self, cell, rowDimensions, rowProperties) :
        """
        Create relevant triples for the cell marked as RowHeader
        """
        if cell['isEmpty']:
            return

        # Get the row        
        i = cell['i']
        # Get the property for the column
        j = cell['j']
        prop = rowProperties[j]

        rowDimensions.setdefault(i, {})
        rowDimensions[i][prop] = cell['value']
 
    
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
        
        if (cell['isEmpty'] or cell['value'].lower() == 'id.') :
            # If the cell is empty, and a HierarchicalRowHeader, add the value of the row header above it.
            # If the cell above is not in the rowValues, don't do anything.
            # If the cell is exactly 'id.', add the value of the row header above it. 
            try :
                rowDimensions.setdefault(i, {})
                rowDimensions[i][prop] = rowDimensions[i - 1][prop]
                # self.log.debug("({},{}) Copied from above\nRow hierarchy: {}".format(i, j, rowValues[i]))
            except :
                pass
                # REMOVED because of double slashes in uris
                # self.rowValues[i][j] = self.source_cell.value
                # self.log.debug("({},{}) Top row, added nothing\nRow hierarchy: {}".format(i, j, rowValues[i]))
        elif cell['value'].lower().startswith('id') and len(cell['value'].lower()) == 3:
            # If the cell starts with 'id.' or 'id ', add the value of the row  above it, and append the rest of the cell's value.
            suffix = cell['value'][3:]               
            try :       
                rowDimensions[i][prop] = rowDimensions[i - 1][prop] + suffix
                # self.log.debug("({},{}) Copied from above+suffix\nRow hierarchy {}".format(i, j, rowValues[i]))
            except :
                rowDimensions[i][prop] = cell['value']
                # self.log.debug("({},{}) Top row, added value\nRow hierarchy {}".format(i, j, rowValues[i]))
        elif not cell['isEmpty']:
            rowDimensions.setdefault(i, {})
            rowDimensions[i][prop] = cell['value']
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
            # Add a new dimension to the graph
            self.log.debug("({},{}) Add column dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
            self.graph.add((cell['URI'], RDF.type, self.conf.getURI('tablink', 'ColumnHeader')))
            # self.graph.add((cell['URI'], RDF.type, RDF['Property']))
            # self.graph.add((cell['URI'], RDF.type, self.conf.getURI('qb', 'DimensionProperty')))
            self.graph.add((cell['URI'], RDFS.label, Literal(cell['value'])))
            self.graph.add((cell['URI'], self.conf.getURI('tablink', 'cell'), Literal(cell['name'])))
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
            # Add a new dimension to the graph
            self.log.debug("({},{}) Add property dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
            self.graph.add((cell['URI'], RDF.type, self.conf.getURI('tablink', 'RowProperty')))
            # self.graph.add((cell['URI'], RDF.type, RDF['Property']))
            # self.graph.add((cell['URI'], RDF.type, self.conf.getURI('qb', 'DimensionProperty')))
            self.graph.add((cell['URI'], RDFS.label, Literal(cell['value'])))
            self.graph.add((cell['URI'], self.conf.getURI('tablink', 'cell'), Literal(cell['name'])))
            dimension = cell['URI']
            
        rowProperties[cell['j']] = dimension        
    
    def handleTitle(self, cell) :
        """
        Create relevant triples for the cell marked as Title 
        """
        self.graph.add((cell['datasetURI'], self.conf.getURI('tablink', 'title'), Literal(cell['value'])))        
    
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
                        self.conf.getURI('oa','Annotation')
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa','hasTarget'),
                        cell['URI']
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa','hasBody'),
                        body
                        ))
        self.graph.add((body,
                        RDF.value,
                        Literal(annot.text.replace("\n", " ").replace("\r", " ").replace("\r\n", " ").encode('utf-8'))
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa','annotatedBy'),
                        Literal(annot.author.encode('utf-8'))
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa','serializedBy'),
                        URIRef("https://github.com/Data2Semantics/TabLinker")
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa','serializedAt'),
                        Literal(datetime.datetime.now().strftime("%Y-%m-%d"), datatype=self.conf.getURI('xsd','date'))
                        ))
        self.graph.add((annotation,
                        self.conf.getURI('oa','modelVersion'),
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
         
if __name__ == '__main__':
    config = Configuration('config.ini')
    
    # Test
    inputFile = "data-test/simple.xls"
    markingFile = "data-test/simple-marking.txt"
    dataFile = "/tmp/data.ttl"

    tLinker = TabLinker(config, inputFile, markingFile, dataFile, processAnnotations = True)
    tLinker.doLink()
    

        
