import os
import re
import sys
from odf.opendocument import load
from odf.table import Table, TableRow
from odf import office
from odf.text import P
from odf.namespaces import TABLENS
from modules.tablinker.helpers import colName, getColumns
from rdflib.term import Literal
from util.sparql import SPARQLWrap

def curify(self, string):
    for (name, value) in self.namespaces.iteritems():
        if string.startswith(value):
            return string.replace(value, name + ':')
    return string

# Define the logger
import logging
log = logging.getLogger(__name__)

QUERY_ANNOTATIONS = """
PREFIX oa: <http://www.w3.org/ns/openannotation/core/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX tablinker: <http://bit.ly/cedar-tablink#>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?cell_name ?p ?o FROM __RULES__ FROM __RAW_DATA__ WHERE {
    [] a oa:Annotation ;
        oa:hasBody [?p ?o];
        oa:hasTarget ?cell.
    ?cell rdfs:label ?cell_name.
    ?cell tablinker:sheet ?sheet.
    ?ds a dcat:DataSet.
    ?ds dcterms:hasPart ?sheet.
    ?ds prov:wasDerivedFrom ?source.
    ?source dcat:distribution [rdfs:label __FILE_NAME__]
}
"""

class RulesInjector(object):
    def __init__(self, end_point, rules_graph, raw_data_graph):
        """
        Constructor
        """
        # Variables
        self.end_point = end_point
        self.rules_graph = rules_graph
        self.raw_data_graph = raw_data_graph
        
    def process_workbook(self, input_file_name, output_file_name):
        """
        Start processing all the sheets in workbook
        """
        # Base name for logging
        basename = os.path.basename(input_file_name)
        
        # Load the book
        log.info('[{}] Loading {}'.format(basename, input_file_name))
        book = load(unicode(input_file_name))
        
        # Go!
        log.debug('[{}] Starting RulesInjector'.format(basename))
        sheets = book.getElementsByType(Table)
        
        # Process all the sheets
        log.info('[{}] Found {} sheets to process'.format(basename, len(sheets)))
        for n in range(len(sheets)) :
            log.debug('[{}] Processing sheet {}'.format(basename, n))
            try:
                self._process_sheet(basename, n, sheets[n])
            except Exception as detail:
                log.error("[{}] Error processing sheet {} : {}".format(basename, n, detail))

        book.save(unicode(output_file_name))
        
    def _process_sheet(self, basename, n, sheet):
        """
        Process a sheet 
        """        
        log.debug('[{}] Load rules'.format(basename))
        annotations_map = {}
        # SPARQL Wrapper
        self.sparql = SPARQLWrap(self.end_point)
        sparql_params = {'__RULES__': self.rules_graph,
                         '__RAW_DATA__' : self.raw_data_graph,
                         '__FILE_NAME__' : Literal(basename).n3()}
        results = self.sparql.run_select(QUERY_ANNOTATIONS, sparql_params)
        for result in results:
            cell_name = result['cell_name']['value'].split('=')[0]
            po_pair = '{}={}'.format(result['p']['value'], result['o']['value'])
            
            annotations_map.setdefault(cell_name, office.Annotation())
            annot = annotations_map[cell_name]
            annot.addElement(P(text=po_pair))
            
        log.debug('[{}] Inject the annotations'.format(basename))
        rows = sheet.getElementsByType(TableRow)
        for rowIndex in range(0, len(rows)):
            cols = getColumns(rows[rowIndex])
            for colIndex in range(0, len(cols)):
                cell_obj = cols[colIndex]
                if cell_obj == None:
                    continue
                        
                # Get the cell name and the current style
                cell_name = colName(colIndex) + str(rowIndex + 1)
                
                if cell_name in annotations_map:
                    annot = annotations_map[cell_name]
                    log.debug('[{}] {} => {}'.format(basename, cell_name, annot))
                    cell_obj.addElement(annot)
