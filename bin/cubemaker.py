if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Set the name of a data set to test
    # BRT_1889_05_T4-S0
    # BRT_1899_03_T-S0 <- Too big ?
    # BRT_1909_02A1_T1-S1 is broken
    # sheet_uri = config.getURI('cedar', 'BRT_1889_05_T4-S0')
    sheet_uri = config.getURI('cedar', 'VT_1859_01_H1-S6')

    # Test
    cube = CubeMaker(config)
    cube.process(sheet_uri, "/tmp/data.ttl")
    cube.generate_dsd("/tmp/extra.ttl")
    
