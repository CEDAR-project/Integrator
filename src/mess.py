#!/usr/bin/python
"""
Extract and ingest styles into Excel files
"""
from common.configuration import Configuration
from xlrd import open_workbook
from xlutils.styles import Styles
from xlutils.margins import number_of_good_rows, number_of_good_cols
from xlrd.formula import cellname
from xlutils.copy import copy
from xlwt import Style
import logging
import bz2

class Mess(object):
    def __init__(self, config, xlsFileName):
        self.log = logging.getLogger("Mess")
        
        # Save a pointer to the configuration
        self.config = config
        
        # Load the workbook
        self.log.debug('Loading Excel file {0}'.format(xlsFileName))
        self.workbook = open_workbook(xlsFileName, formatting_info=True)
        self.styles = Styles(self.workbook)
    
    def extractMarkingFromXLS(self, mrkFileName):
        '''
        Process the current Excel file to extract the marking annotation
        and write the output to mrkFileName
        '''
        outputHandle = bz2.BZ2File(mrkFileName + ".bz2", 'wb', compresslevel=9) if config.isCompress() else open(mrkFileName, "w")
        for n in range(self.workbook.nsheets):
            sheet = self.workbook.sheet_by_index(n)
            colns = number_of_good_cols(sheet)
            rowns = number_of_good_rows(sheet)
            self.log.debug("Process %d columns and %d rows" % (colns, rowns))
            for i in range(0, rowns):
                for j in range(0, colns):
                    cell = sheet.cell(i, j)
                    cell_name = cellname(i, j)
                    style_name = self.styles[cell].name
                    if style_name.startswith('TL '):
                        mrk_line = "%d;%s;%s" % (n, cell_name, style_name)
                        outputHandle.write(mrk_line)
                        outputHandle.write("\n")
        outputHandle.close()
          
    def injectMarkingIntoXLS(self, mrkFileName, targetFileName):
        '''
        Load marking instructions from mrkFileName and process the current
        Excel file to generate the annotated targetFileName
        '''
        self.log.debug("Copy book")
        # Prepare the output
        target_workbook = copy(self.workbook)
        
        self.log.debug("Load marking from %s" % mrkFileName)
        # Load marking information
        marking = {}
        style_names = []
        for mrk in open(mrkFileName):
            (index_str, cell, style) = mrk.strip().split(';')
            index = int(index_str)
            marking.setdefault(index, {})
            marking[index][cell] = style
            if style not in style_names:
                style_names.append(style)    

        # Create the styles in the target
        # TODO
        
        # Process the source workbook
        for n in range(self.workbook.nsheets):
            sheet = self.workbook.sheet_by_index(n)
            target_sheet = target_workbook.get_sheet(n)
            colns = number_of_good_cols(sheet)
            rowns = number_of_good_rows(sheet)
            self.log.debug("Process %d columns and %d rows" % (colns, rowns))
            for i in range(0, rowns):
                for j in range(0, colns):
                    cell = sheet.cell(i, j)
                    cell_name = cellname(i, j)
                    cell_xf_index = sheet.cell_xf_index(i, j)
                    print cell_xf_index
                    if cell_name in marking[n]:
                        # TODO Use matching style defined earlier
                        target_sheet.write(i, j, label=cell.value, style=Style.default_style)
                        
        target_workbook.save(targetFileName)
    
if __name__ == '__main__':
    config = Configuration('config.ini')
    
    # Test on one file
    inputFile = "data-test/simple.xls"
    markingFile = "data-test/simple-marking.txt"
    mess = Mess(config, inputFile)
    mess.extractMarkingFromXLS(markingFile)
    
    # Used once to extract all the marking
    #for inputFile in sorted(glob.glob("/home/cgueret/Code/CEDAR/DataDump/xls-marked/*.xls")):
    #    name = os.path.basename(inputFile)
    #    name = re.search('(.*)\.xls',name).group(1).replace("_marked", "")
    #    markingFile = "/home/cgueret/Code/CEDAR/Harmonize/data/input/marking/" + name + '.txt'
    #    mess = Mess(config, inputFile)
    #    mess.extractMarkingFromXLS(markingFile)
        
