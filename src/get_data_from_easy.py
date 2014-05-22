#!/usr/bin/python2
# Useful doc http://www.pythonforbeginners.com/cheatsheet/python-mechanize-cheat-sheet
import mechanize
import re
import os
from bs4 import BeautifulSoup

def go():
    # Load credentials (login first line, password second line)
    credentials = [c.strip() for c in open('credentials.txt')]
    
    br = mechanize.Browser()
    br.open("https://easy.dans.knaw.nl/ui/home")
    # Log in
    br.follow_link(text="Log in")    
    br.form = list(br.forms())[2] # The form with the login is the third one
    br.form.find_control("userId").value = credentials[0]
    br.form.find_control("credentials").value = credentials[1]
    br.submit()
    # Search for the census data
    br.form = list(br.forms())[0] # Search is the first form
    br.form.find_control("searchString").value = "\"Website: Volkstellingen 1795-1971\""
    response = br.submit()
    more_pages = True
    result_hrefs = []
    while more_pages:
        # Process one page of results
        soup = BeautifulSoup(response.read())
        for result in soup.find_all('div', 'searchHit'):
            # Extract the link from the Javascript code
            target = result.get('onclick')
            m = re.search("window\.location\.href='([^']*)'", target)
            href = 'https://easy.dans.knaw.nl/ui/' + m.group(1) 
            result_hrefs.append(href)
        # See if there is another page
        more_pages = 'next >' in [link.text for link in br.links()]
        if more_pages:
            response = br.follow_link(text='next >') 
    # Open all the results one by one
    for result_href in result_hrefs:
        # Open the page
        response = br.open(result_href)
        title = BeautifulSoup(response.read()).find_all('h1')[0].string
        print '# Now parsing data set : ' + title
        
        # Find the link to the data files
        data_link_text = None
        for l in [link.text for link in br.links()]:
            if l.startswith('Data files'):
                data_link_text = l
        # Open the data tab
        response = br.follow_link(text=data_link_text)
        soup = BeautifulSoup(response.read())
        parsed_link_label = set()
        for result in soup.find_all('a'):
            spans = result.find_all('span')
            if len(spans) == 0:
                continue
            link_label = spans[0].string
            # Try to find a folder with the data we need
            if link_label in ['Beroepentellingen', 'Volkstellingen', 'Woningtellingen'] and link_label not in parsed_link_label:
                parsed_link_label.add(link_label)
                # Open the folder
                target = result.get('onclick')
                m = re.search("wicketAjaxGet\('([^']*)'", target)
                href = 'https://easy.dans.knaw.nl/ui/' + m.group(1).replace('../../../../', '')
                response_ajax = br.open(href)
                soup_ajax = BeautifulSoup(response_ajax.read())
                # Now try to find the subfolder with the Excel files
                href = None
                for result2 in soup_ajax.find_all('a'):
                    spans = result2.find_all('span')
                    if len(spans) == 0:
                        continue
                    if spans[0].string == 'Excel':
                        target = result2.get('onclick')
                        m = re.search("wicketAjaxGet\('([^']*)'", target)
                        href = 'https://easy.dans.knaw.nl/ui/' + m.group(1).replace('../../../../', '')
                if href==None:
                    print "No Excel folder for " + link_label
                    continue
                # Open the folder
                response_ajax = br.open(href)
                soup_ajax = BeautifulSoup(response_ajax.read())
                # Find all the links to Excel files
                for link in soup_ajax.find_all('a'):
                    spans = link.find_all('span')
                    if len(spans) == 0:
                        continue
                    if spans[0].string.endswith('.xls'):
                        excel_file_name = spans[0].string
                        target = link.get('onclick')
                        m = re.search("wicketAjaxGet\('([^']*)'", target)
                        href = 'https://easy.dans.knaw.nl/ui/' + m.group(1)
                        response_ajax = br.open(href).read()
                        m = re.search("window\.location\.href='([^']*)'", response_ajax)
                        href = 'https://easy.dans.knaw.nl/ui/' + m.group(1)
                        if not os.path.isfile('data/' + excel_file_name):
                            print 'Saving ' + excel_file_name
                            excel_file_content = br.open(href).read()
                            f = open ('data/' + excel_file_name, "wb")
                            f.write(excel_file_content)
                            f.close()
        
if __name__ == '__main__':
    go()