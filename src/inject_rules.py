import os
import re
import sys
from odf.opendocument import load
from odf.table import Table, TableRow
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from odf import office
from odf.text import P
from odf.namespaces import TABLENS

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


class RulesInjector(object):
    def __init__(self, conf, input_file_name, output_file_name):
        """
        Constructor
        """
        # Variables
        self.conf = conf
        self.log = conf.getLogger("RulesInjector")
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.basename = os.path.basename(input_file_name)
        self.basename = re.search('(.*)\.ods', self.basename).group(1)
        
        # SPARQL Wrapper
        self.sparql = SPARQLWrap(self.conf)
        
    
    def process_workbook(self):
        """
        Start processing all the sheets in workbook
        """
        # Load the book
        self.log.info('[{}] Loading {}'.format(self.basename, self.input_file_name))
        book = load(unicode(self.input_file_name))
        
        # Go!
        self.log.debug('[{}] Starting RulesInjector'.format(self.basename))
        sheets = book.getElementsByType(Table)
        
        # Process all the sheets
        self.log.info('[{}] Found {} sheets to process'.format(self.basename, len(sheets)))
        for n in range(len(sheets)) :
            self.log.debug('[{}] Processing sheet {}'.format(self.basename, n))
            try:
                self._process_sheet(n, sheets[n])
            except Exception as detail:
                self.log.error("[{}] Error processing sheet {}".format(self.basename, n))
                self.log.error(sys.exc_info()[0])
                self.log.error(detail)

        book.save(unicode(self.output_file_name))
        
    def _process_sheet(self, n, sheet):
        """
        Process a sheet 
        """        
        # Define a sheetURI for the current sheet
        mapping_activity_URI = self.conf.getURI('cedar', "{0}-S{1}-mapping-activity".format(self.basename, n))        
        
        self.log.debug('[{}] Load rules associated to {}'.format(self.basename, mapping_activity_URI))
        annotations_map = {}
        sparql_query = open('Queries/get_rules.sparql', 'r').read()
        sparql_params = {'__MAPPING_ACTIVITY__': mapping_activity_URI}
        results = self.sparql.run_select(sparql_query, sparql_params)
        for result in results:
            cell_name = result['target']['value'].split('-')[-1]
            po_pair = '{}={}'.format(self.conf.curify(result['p']['value']),
                                     self.conf.curify(result['o']['value']))
            
            annotations_map.setdefault(cell_name, office.Annotation())
            annot = annotations_map[cell_name]
            annot.addElement(P(text=po_pair))
            
        self.log.debug('[{}] Inject the annotations'.format(self.basename))
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
                    self.log.debug('[{}] {} => {}'.format(self.basename, cell_name, annot))
                    cell_obj.addElement(annot)
    
        
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    config.setVerbose(True)
    
    # Set target files for testing
    name = 'VT_1859_01_H1'
    name = 'VT_1899_07_H1'
    input_file_name = 'DataDump/source-data/{}.ods'.format(name)
    output_file_name = '/tmp/{}.ods'.format(name)
    
    # Test
    rules_injector = RulesInjector(config, input_file_name, output_file_name)
    rules_injector.process_workbook() 
    
