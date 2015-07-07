#!/usr/bin/python2
from util.configuration import Configuration
from integrat import Integrator

# Create the logger
import logging
log = logging.getLogger(__name__)

if __name__ == '__main__':
    # Load the configuration file
    config = Configuration('config.ini')
    
    # Configure the logger
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG if config.verbose() else logging.INFO)
    logFormat = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'    
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    fh = logging.FileHandler('integrator.log', mode='w')
    fh.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(fh)
    
    # Create an instance of the integrator
    integrator = Integrator(config)
        
    # Step 1 : convert the spreadsheet files
    integrator.generate_raw_data()

    # Step 2 : generate harmonisation rules
    integrator.generate_harmonization_rules()
    
    # Step 3 (optional) : generate enhanced coloured files
    integrator.generate_enriched_source_files()
    
    # Step 4 : generate the release data cube, the DSD and the slices
    integrator.generate_release()
    