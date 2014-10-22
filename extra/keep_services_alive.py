#!/usr/bin/python2

import requests
from systemd import journal

SPARQL = "http://lod.cedar-project.nl/cedar/sparql"
TEST_QUERY = "SELECT * WHERE {?s ?p ?o} LIMIT 1"
TEST_RESOURCE = "http://lod.cedar-project.nl:8888/cedar/resource/harmonised-data-dsd"

if __name__ == '__main__':
    # Check virtuoso
    r = requests.post(SPARQL, data={'query':TEST_QUERY})
    if r.status_code != 200:
        systemd.journal.send('Restart Virtuoso')
        subprocess.call("systemctl restart virtuoso", shell=True)
    else:
        journal.send('Virtuoso is doing fine')
        
    # Check tomcat
    r = requests.get(TEST_RESOURCE)
    if r.status_code != 200:
        systemd.journal.send('Restart Tomcat')
        subprocess.call("systemctl restart tomcat6", shell=True)
        systemd.journal.send('Restart Apache')
        subprocess.call("systemctl restart httpd", shell=True)
    else:
        journal.send('Tomcat is doing fine')
    
