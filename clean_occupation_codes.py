import csv
import re

def _clean_string(text):
    text_clean = text.replace('.', '').replace('_', ' ').lower()
    text_clean = re.sub(r'\s+', ' ', text_clean)
    text_clean = text_clean.strip()
    return text_clean

if __name__ == '__main__':
    # Load and clean the data
    occupations = {}
    file  = open('raw_beroep_data.csv', "rb")
    reader = csv.reader(file)
    for row in reader:
        uri = 'http://cedar.example.org/ns#hisco-' + row[1]
        occupations[_clean_string(row[0])] = uri
    file.close()
    
    # Write the output to the codes file
    file  = open('codes/occupation.csv', "wb")
    file.write("\"Literal\",Code\n")
    for occupation in sorted(occupations.keys()):
        row = "\""+occupation+"\","+occupations[occupation]+"\n"
        file.write(row)
    file.close()
    