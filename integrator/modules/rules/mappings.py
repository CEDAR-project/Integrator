import os

from rdflib.term import URIRef
from xlrd import open_workbook
from xlutils.margins import number_of_good_cols, number_of_good_rows

class MappingsList(object):
    
    def __init__(self, data, configuration):
        self._mappings = {}
        
        self.excelFileName = data['file']
        predicate = URIRef(data['predicate'])
        mapping_type = data['mapping_type']
        
        # Load the mappings
        wb = open_workbook(data['path'] + "/" + self.excelFileName, formatting_info=False, on_demand=True)
        sheet = wb.sheet_by_index(0)
        colns = number_of_good_cols(sheet)
        rowns = number_of_good_rows(sheet)
        for i in range(1, rowns):
            # Get the context (first column)
            context = sheet.cell(i, 0).value
            
            # Get the string (force reading the cell as a string)
            literal = sheet.cell(i, 1).value
            if type(literal) == type(1.0):
                literal = str(int(literal))
            #literal = util.clean_string(literal)
            
            # Get the values
            values = []
            for j in range(2, colns):
                value = sheet.cell(i, j).value
                if value != '':
                    # Codes using numbers need to be seen as string
                    if type(value) == type(1.0):
                        value = str(int(value))
                        
                    # Encode the value
                    encoded_value = None
                    if mapping_type == 'uri':
                        prefix = data['prefix']
                        encoded_value = URIRef(prefix + value)
                    elif mapping_type == 'boolean':
                        isTrue = (value == '1' or value == 'true')
                        encoded_value = Literal(isTrue)
                    else:
                        encoded_value = Literal(value)
                        
                    # Prefix the code and pair with predicate
                    pair = (predicate, encoded_value)
                    values.append(pair)
                    
            if len(values) == 0:
                values = None
                
            # Save the mapping
            self._mappings.setdefault(literal, {})
            if context != '':
                # Store the specific context
                self._mappings[literal].setdefault('context', {})
                self._mappings[literal]['context'][context] = values
            else:
                # Store the default mappings
                self._mappings[literal]['default'] = values 
                    
        
    def get_src_URI(self):
        """
        Return a URI for the Excel file
        """
        root = URIRef("https://raw.githubusercontent.com/CEDAR-project/DataDump/master/mapping/")
        mappingsDumpURI = root + os.path.relpath(self.excelFileName)
        return URIRef(mappingsDumpURI)
    
    def get_file_name(self):
        return self.excelFileName
    
    def get_mappings_for(self, literal, context_map):
        '''
        Returns a set of pairs for a given string
        '''
        # If we don't have a mapping return nothing
        if literal not in self._mappings:
            return None
        
        # If there is no context cases just return the default
        if 'context' not in self._mappings[literal]:
            return self._mappings[literal]['default']
        
        # Check for all the possible context, starting with the most specific
        for key in ['cell','sheet','dataset']:
            context = '%s=%s' % (key, context_map[key])
            if context in self._mappings[literal]['context']:
                return self._mappings[literal]['context'][context]
        
        # Current context match no exception for this literal
        # If no default mapping is defined, don't map the literal
        if 'default' not in self._mappings[literal]:
            return None
        
        # Exceptions don't match but we have a default mapping
        return self._mappings[literal]['default']
