"""
TabLinker
"""
import sys
import exceptions
reload(sys)
import traceback
sys.setdefaultencoding("utf8")  # @UndefinedVariable

import bz2
import os.path
import datetime

from namespace import TABLINKER, DCAT, PROV, OA
from helpers import getColumns, colName, getText, clean_string

from rdflib import ConjunctiveGraph, Literal, URIRef, Namespace
from rdflib.namespace import RDF, RDFS, XSD, DCTERMS

from odf import text, office, dc
from odf.opendocument import load
from odf.table import Table, TableRow
from odf.namespaces import TABLENS, STYLENS
from odf.style import Style

import logging
logger = logging.getLogger(__name__)


class TabLinker(object):
    def __init__(self, input_file_name, output_file_name, processAnnotations=False):
        """
        Constructor
        """
        # Save the arguments
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.processAnnotations = processAnnotations
        
        # Create the graph
        self.graph = ConjunctiveGraph()
        self.graph.bind('tablinker', TABLINKER)
        self.graph.bind('prov', PROV)
        self.graph.bind('dcat', DCAT)
        self.graph.bind('oa', OA)
        self.graph.bind('dcterms', DCTERMS)

        # Set a default namespace
        self.data_ns = Namespace("http://example.org/")
        self.graph.bind('data', self.data_ns)
        
        # Compress by default
        self.set_compress(True)
        
        self.basename = os.path.basename(input_file_name).split('.')[0]
                
        logger.info('[{}] Loading {}'.format(self.basename, input_file_name))
        self.book = load(unicode(input_file_name))
        self.stylesnames = {}
        for style in self.book.getElementsByType(Style):
            parentname = style.getAttrNS(STYLENS, 'parent-style-name')
            name = style.getAttrNS(STYLENS, 'name')
            if parentname != None:
                self.stylesnames[name] = parentname
            
    def set_target_namespace(self, namespace):
        """
        Set the target namespace used to prefix all the resources of the
        data generated
        """
        self.data_ns = Namespace(namespace)
        self.graph.namespace_manager.bind('data', self.data_ns, 
                                          override=True, replace=True)
    
    def set_compress(self, value):
        """
        Set the usage of compression on or off
        """
        self.compress_output = value
        
    def doLink(self):
        """
        Start processing all the sheets in workbook
        """
        logger.debug('[{}] Starting TabLink for all sheets in workbook'.format(self.basename))
        # keep the starting time (ex "2012-04-15T13:00:00-04:00")
        startTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=XSD.dateTime)

        sheets = self.book.getElementsByType(Table)
        
        # Process all the sheets
        logger.info('[{}] Found {} sheets to process'.format(self.basename,len(sheets)))
        sheetURIs = []
        for n in range(len(sheets)) :
            logger.debug('Processing sheet {0}'.format(n))
            try:
                (sheetURI, marked_count) = self.parseSheet(n, sheets[n])
                if marked_count != 0:
                    # Describe the sheet
                    self.graph.add((sheetURI, RDF.type, TABLINKER.Sheet))
                    self.graph.add((sheetURI, RDFS.label, Literal(sheetURI.replace(self.data_ns, ''))))
                    self.graph.add((sheetURI, TABLINKER.value, Literal(sheets[n].getAttrNS(TABLENS, 'name'))))
                    # Add it to the dataset
                    sheetURIs.append(sheetURI)
            except Exception as detail:
                logger.error("Error processing sheet %d of %s" % (n, self.basename))
                logger.error(sys.exc_info()[0])
                logger.error(detail)
            
        # end time for the conversion process
        endTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                          datatype=XSD.dateTime)
        
        # Mint the URIs
        datasetURI = self.data_ns["{0}".format(self.basename)]
        distURI = self.data_ns["{0}-dist".format(self.basename)]
        activityURI = self.data_ns["{0}-tablink".format(self.basename)]
        srcURI = self.data_ns["{0}-src".format(self.basename)]
        srcdistURI = self.data_ns["{0}-src-dist".format(self.basename)]
        root = URIRef("https://raw.githubusercontent.com/CEDAR-project/DataDump/master/")
        datasetDumpURI = root + os.path.relpath(self.output_file_name)
        if self.compress_output:
            datasetDumpURI = datasetDumpURI + '.bz2' 
        excelFileURI = root + self.input_file_name
        
        # Describe the data set
        self.graph.add((datasetURI, RDF.type, DCAT.DataSet))
        self.graph.add((datasetURI, RDFS.label, Literal(self.basename)))
        self.graph.add((datasetURI, PROV.wasDerivedFrom, srcURI))
        self.graph.add((datasetURI, PROV.wasGeneratedBy, activityURI))
        for sheetURI in sheetURIs:
            self.graph.add((datasetURI, DCTERMS.hasPart, sheetURI))
        self.graph.add((datasetURI, DCAT.distribution, distURI))
        
        # Describe the distribution of the dataset
        self.graph.add((distURI, RDF.type, DCAT.Distribution))
        dumpname = os.path.basename(self.output_file_name)
        if self.compress_output:
            dumpname = dumpname + '.bz2'
        self.graph.add((distURI, RDFS.label, Literal(dumpname)))
        self.graph.add((distURI, DCTERMS.accessURL, datasetDumpURI))
        
        # Describe the source of the dataset
        self.graph.add((srcURI, RDF.type, DCAT.DataSet))
        self.graph.add((srcURI, RDFS.label, Literal(os.path.basename(self.input_file_name))))
        self.graph.add((srcURI, DCAT.distribution, srcdistURI))
        self.graph.add((srcURI, TABLINKER.sheets, Literal(len(sheets))))
        
        # Describe the distribution of the source of the dataset
        self.graph.add((srcdistURI, RDF.type, DCAT.Distribution))
        self.graph.add((srcdistURI, RDFS.label, Literal(os.path.basename(self.input_file_name))))
        self.graph.add((srcdistURI, DCTERMS.accessURL, excelFileURI))
        
        # The activity is the conversion process
        self.graph.add((activityURI, RDF.type, PROV.Activity))
        self.graph.add((activityURI, PROV.startedAtTime, startTime))
        self.graph.add((activityURI, PROV.endedAtTime, endTime))
        self.graph.add((activityURI, PROV.wasAssociatedWith, TABLINKER.tabLink))
        self.graph.add((activityURI, PROV.used, srcURI))
        
        # Save the graph
        logger.info('[{}] Saving {} data triples'.format(self.basename,len(self.graph)))
        try :
            out = bz2.BZ2File(self.output_file_name + '.bz2', 'wb', compresslevel=9) if self.compress_output else open(self.output_file_name, "w")
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
        sheetURI = self.data_ns["{0}-S{1}".format(self.basename, n)]       
        
        columnDimensions = {}
        row_dims = {}
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
                
                # logger.debug("({},{}) {}/{}: \"{}\"". format(i, j, cellType, cellName, cellValue))

                # Increase the counter of marked cells
                if cell['type'] in ['TL Data', 'TL RowHeader', 'TL HRowHeader', 'TL ColHeader', 'TL RowProperty']:
                    marked_count = marked_count + 1
                    
                # Parse cell content
                if cell['type'] == 'TL Data':
                    self.handleData(cell, columnDimensions, row_dims)
                elif cell['type'] == 'TL RowHeader' :
                    self.handleRowHeader(cell, row_dims, rowProperties)
                elif cell['type'] == 'TL HRowHeader' :
                    self.handleHRowHeader(cell, row_dims, rowProperties)
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
        for rowDimension in row_dims:
                for (p, vs) in row_dims[rowDimension].iteritems():
                    for v in vs:
                        try:
                            self.graph.add((v, TABLINKER.parentCell, p))
                        except exceptions.AssertionError:
                            logger.debug('Ignore {}'.format(p))
                            
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
    
    def handleData(self, cell, columnDimensions, row_dims) :
        """
        Create relevant triples for the cell marked as Data
        """
        if cell['isEmpty']:
            return
        
        logger.debug("({},{}) Handle data cell".format(cell['i'], cell['j']))
                
        # Add the cell to the graph
        self._create_cell(cell, TABLINKER.DataCell)
            
        # Bind all the row dimensions
        try :
            for dims in row_dims[cell['i']].itervalues():
                for dim in dims:
                    self.graph.add((cell['URI'], TABLINKER.dimension, dim))
        except KeyError :
            logger.debug("({},{}) No row dimension for cell".format(cell['i'], cell['j']))
        
        # Bind all the column dimensions
        try :
            for dim in columnDimensions[cell['j']]:
                self.graph.add((cell['URI'], TABLINKER.dimension, dim))
        except KeyError :
            logger.debug("({},{}) No column dimension for cell".format(cell['i'], cell['j']))
        
    def handleRowHeader(self, cell, row_dims, rowProperties) :
        """
        Create relevant triples for the cell marked as RowHeader
        """
        if cell['isEmpty']:
            return

        logger.debug("({},{}) Handle row header : {}".format(cell['i'], cell['j'], cell['value']))
        
        # Add the cell to the graph
        self._create_cell(cell, TABLINKER.RowHeader)
        
        # Get the row        
        i = cell['i']
        # Get the property for the column
        j = cell['j']
        try:
            prop = rowProperties[j]
        except exceptions.KeyError:
            prop = 'NonExistingRowHeader%d' % j 
        
        row_dims.setdefault(i, {})
        row_dims[i].setdefault(prop, [])
        row_dims[i][prop].append(cell['URI'])
        
        # Look if we cover other cells verticaly 
        rows_spanned = cell['cell'].getAttrNS(TABLENS, 'number-rows-spanned')
        if rows_spanned != None:
            rows_spanned = int(rows_spanned)
            for extra in range(1, rows_spanned):
                spanned_row = cell['i'] + extra
                logger.debug("Span over ({},{})".format(spanned_row, cell['j']))
                row_dims.setdefault(spanned_row, {})
                row_dims[spanned_row].setdefault(prop, [])
                row_dims[spanned_row][prop].append(cell['URI'])
 
    
    def handleHRowHeader(self, cell, row_dims, rowProperties) :
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
        
        logger.debug("({},{}) Handle HRow header".format(cell['i'], cell['j']))
        
        if (cell['isEmpty'] or cell['value'].lower() == 'id.' or cell['value'].lower() == 'id ') :
            # If the cell is empty, and a HierarchicalRowHeader, add the value of the row header above it.
            # If the cell is exactly 'id.', add the value of the row header above it.
            try:
                row_dims.setdefault(i, {})
                row_dims[i].setdefault(prop, [])
                row_dims[i][prop].append(row_dims[i - 1][prop][0])
            except:
                pass
            # logger.debug("({},{}) Copied from above\nRow hierarchy: {}".format(i, j, rowValues[i]))
        elif not cell['isEmpty']:
            # Add the cell to the graph
            self._create_cell(cell, TABLINKER.RowHeader)
            row_dims.setdefault(i, {})
            row_dims[i].setdefault(prop, [])
            row_dims[i][prop].append(cell['URI'])
            # logger.debug("({},{}) Added value\nRow hierarchy {}".format(i, j, rowValues[i]))

        # Look if we cover other cells verticaly 
        rows_spanned = cell['cell'].getAttrNS(TABLENS, 'number-rows-spanned')
        if rows_spanned != None:
            rows_spanned = int(rows_spanned)
            for extra in range(1, rows_spanned):
                spanned_row = cell['i'] + extra
                logger.debug("Span over ({},{})".format(spanned_row, cell['j']))
                row_dims.setdefault(spanned_row, {})
                row_dims[spanned_row].setdefault(prop, [])
                row_dims[spanned_row][prop].append(cell['URI'])
    
    def handleColHeader(self, cell, columnDimensions) :
        """
        Create relevant triples for the cell marked as Header
        """
        # Add the col header to the graph
        logger.debug("({},{}) Add column header \"{}\"".format(cell['i'], cell['j'], cell['value']))
        self._create_cell(cell, TABLINKER.ColumnHeader)
        
        # If there is already a parent dimension, connect to it
        if cell['j'] in columnDimensions:
            self.graph.add((cell['URI'], TABLINKER.parentCell, columnDimensions[cell['j']][-1]))    
        dimension = cell['URI']
            
        # Add the dimension to the dimensions list for that column
        columnDimensions.setdefault(cell['j'], []).append(dimension)
        
        # Look if we cover other cells
        columns_spanned = cell['cell'].getAttrNS(TABLENS, 'number-columns-spanned')
        if columns_spanned != None:
            columns_spanned = int(columns_spanned)
            for extra in range(1, columns_spanned):
                spanned_col = cell['j'] + extra
                logger.debug("Span over ({},{})".format(cell['i'], spanned_col))
                columnDimensions.setdefault(spanned_col, []).append(dimension)
        
        
    def handleRowProperty(self, cell, rowProperties) :
        """
        Create relevant triples for the cell marked as Property Dimension
        """
        
        # Add the cell to the graph
        logger.debug("({},{}) Add property dimension \"{}\"".format(cell['i'], cell['j'], cell['value']))
        self._create_cell(cell, TABLINKER.RowProperty)
        rowProperties[cell['j']] = cell['URI']        
        
        # Look if we cover other cells
        columns_spanned = cell['cell'].getAttrNS(TABLENS, 'number-columns-spanned')
        if columns_spanned != None:
            columns_spanned = int(columns_spanned)
            for extra in range(1, columns_spanned):
                logger.debug("Span over ({},{})".format(cell['i'], cell['j'] + extra))
                rowProperties[cell['j'] + extra] = cell['URI']
            
    
    def handleTitle(self, cell) :
        """
        Create relevant triples for the cell marked as Title 
        """
        self.graph.add((cell['sheetURI'],
                        RDFS.comment,
                        Literal(clean_string(cell['value']))))        
    
    def handleAnnotation(self, cell, annotation) :
        """
        Create relevant triples for the annotation attached to the cell
        """
        
        # Create triples according to Open Annotation model
        annotation_URI = cell['URI'] + "-oa"
        annotation_body_URI = annotation_URI + '-body'

        self.graph.add((annotation_URI, RDF.type, OA.Annotation))
        self.graph.add((annotation_URI, OA.hasTarget, cell['URI']))
        self.graph.add((annotation_URI, OA.hasBody, annotation_body_URI))
        
        self.graph.add((annotation_body_URI, RDF.type, RDFS.Resource))
        self.graph.add((annotation_body_URI,
                        TABLINKER.value,
                        Literal(clean_string(getText(annotation)))))
        
        # Extract author
        author = annotation.getElementsByType(dc.Creator)
        if len(author) > 0:
            author = clean_string(str(author[0]))
            self.graph.add((annotation_body_URI, OA.annotatedBy, Literal(author)))
            
        # Extract date
        creation_date = annotation.getElementsByType(dc.Date)
        if len(creation_date) > 0:
            creation_date = str(creation_date[0])
            self.graph.add((annotation_body_URI, OA.serializedAt, Literal(creation_date, datatype=XSD.date)))
            
    def _create_cell(self, cell, cell_type):
        """
        Create a new cell
        """
        
        # Set the value
        value = Literal(clean_string(cell['value']))
            
        # It's a cell
        self.graph.add((cell['URI'], RDF.type, cell_type))
        
        # It's in the data set defined by the current sheet
        self.graph.add((cell['URI'], TABLINKER.sheet, cell['sheetURI']))
        
        # Add its value (removed the datatype=XSD.decimal because we can't be sure)
        self.graph.add((cell['URI'], TABLINKER.value, value))
        
        # Add a cell label
        label = "Cell %s=%s" % (cell['name'], cell['value'])
        self.graph.add((cell['URI'], RDFS.label, Literal(label)))
        
if __name__ == '__main__':
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)
    logFormat = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
        
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    
    # Test
    #inputFile = "data-test/gambia-wages-prices-welfare-ratio.ods"
    #inputFile = "data-test/VT_1899_07_H1.ods"
    inputFile = 'data-test/simple.ods'
    outputFile = "/tmp/data.ttl"

    tLinker = TabLinker(inputFile, outputFile, processAnnotations=True)
    tLinker.set_target_namespace("http://example.com/")
    tLinker.doLink()
    

        
