#!/usr/bin/python2
import requests
import json
from common.sparql import SPARQLWrap
from common.configuration import Configuration

GIST_URL = "https://api.github.com/gists/cf9f09be973cde7cf993"

class QueryCache():
    def __init__(self, config):
        self._conf = config
        self._sparql = SPARQLWrap(config)
        self.log = config.getLogger('QueryCache')
        
    def go(self):
        output = {}
        
        data = requests.get(GIST_URL).json()
        for (name, content) in data['files'].iteritems():
            if name.endswith('.rq'):
                self.log.info("Running " + name)
                results = self._sparql.run_select(content['content'])
                output[name] = results
        
        return output
        
if __name__ == '__main__':
    config = Configuration('config.ini')
    cache = QueryCache(config)
    output = cache.go()
    with open('/tmp/stats.json', 'w') as outfile:
        json.dump(output, outfile)
