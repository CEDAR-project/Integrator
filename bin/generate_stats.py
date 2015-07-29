from util.configuration import Configuration
from modules.reporting.stats import StatsGenerator

import logging
log = logging.getLogger(__name__)

if __name__ == '__main__':
    # Configure a logger
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)
    logFormat = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'    
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    
    # Load the configuration file
    config = Configuration('/home/cgueret/Code/CEDAR/DataDump-mini-vt/config.ini')
    
    # Initialise the stats generator
    statsGenerator = StatsGenerator(config.get_SPARQL(),
                                    config.get_graph_name('raw-data'),
                                    config.get_graph_name('rules'),
                                    config.get_graph_name('release'),
                                    True) # Use the cache to speed up testing
    
    # Go !
    statsGenerator.go('/tmp/stats.html')
