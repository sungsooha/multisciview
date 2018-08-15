CONFIG = {
    # data directory to monitor (recursively)
    # It is safe not to inlcude the last back-slash
    'DATA_DIR': '/Users/scott/Desktop/test3',

    # File path web server will use to store file system information
    'FSMAP': './fsmap.json',

    # mongo db set-up
    'DB': {
        #'HOST': 'visws.csi.bnl.gov',
        'HOST': 'localhost',
        'PORT': 27017
    },

    # parsing xml file
    'XML': {
        # rood id field
        'ROOTID': 'name',

        # sample name split
        # now, user can select seperator or sample name per folder from font-end
        'SAMPLE_SPLIT': ['_th', '_thresh'],

        # protocol id field
        'PID': 'name',

        # for same protocol, use COMPARE field (use only the latest one)
        'TIMESTAMP': 'save_timestamp',

        # result id field
        'RID': 'name',  # id
        'RVAL': 'value',  # value

        # fields that will be excluded in a result
        'R_EXCLUDE': [
            'filebase',
            'file_access_time',
            'sample_name',
            'file_ctime',
            'file_size',
            'infile',
            'filepath',
            'filename',
            'fileext',
            'file_modification_time'
        ],

        # fields whose value will be considered as string
        'R_STRING': [],
    }
}
