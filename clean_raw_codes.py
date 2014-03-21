import csv
import re

def clean_string(text):
    text_clean = text.replace('.', '').replace('_', ' ').lower()
    text_clean = re.sub(r'\s+', ' ', text_clean)
    text_clean = text_clean.strip()
    return text_clean

def clean_codes(source, dest, prefix):
    # Load and clean the data
    occupations = {}
    file  = open(source, "rb")
    reader = csv.reader(file)
    for row in reader:
        uri = 'http://cedar.example.org/ns#' + prefix + '-' + row[1]
        occupations[clean_string(row[0])] = uri
    file.close()
    
    # Write the output to the codes file
    file  = open(dest, "wb")
    file.write("\"Literal\",Code\n")
    for occupation in sorted(occupations.keys()):
        row = "\""+occupation+"\","+occupations[occupation]+"\n"
        file.write(row)
    file.close()
    
if __name__ == '__main__':
    clean_codes('raw_beroep_data.csv', 'codes/occupation.csv', 'hisco')
    clean_codes('raw_religion_data.csv', 'codes/belief.csv', 'belief')
    clean_codes('raw_gemeente_data.csv', 'codes/city.csv', 'ac')
    