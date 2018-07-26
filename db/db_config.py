MONGODB_CONFIG = {
    # deprecated
    'ROOT': '/Users/scott/Documents/Work/bnl/MultiView/pyServer/data/saxs/',

    # data directory to monitor (recursively)
    'DATA_DIR': '/Users/scott/Desktop/test2', # It is safe not to inlcude the last back-slash
    'FSMAP': './fsmap.json',

    # mongo db set-up
    'DB': {
        #'HOST': 'visws.csi.bnl.gov',
        'HOST': 'localhost',
        'PORT': 27017
    },

    # parsing xml file
    'XML': {
        # root directory relative to ROOT
        'DIR': 'analysis_proper/results/',

        # sample name split
        'SAMPLE_SPLIT': '_th0.',

        # for same protocol, use COMPARE field
        'TIMESTAMP': 'save_timestamp',

        # rood id field
        'ROOTID': 'name',

        # protocol id field
        'PID': 'name',

        # result id field
        'RID': 'name',   # id
        'RVAL': 'value', # value

        # fields that will be ignored in a protocol
        'P_EXCLUDE': [
            'infile',
            'outfile',
            'output_dir',
            'runtime',
        ],

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
        'R_STRING': [

        ],

        'TIME_FIELD': [
            'sequence_ID',
            'start_timestamp',
            'end_timestamp',
            'save_timestamp'
        ]
    },

    'TIME': {
        'XML': False,
        'DB': False,
        'FORMAT': '%Y-%m-%d %H:%M:%S %f',
    },


    # tiff (raw data) related
    # CROP defines start row and col index (i.e. the first pixel at upper-left corner)
    'TIFF': {
        'SAVE': True,
        'EXT': ['', '.tiff'],
        'MODIFY': False,
        'DIR': 'tiff/',
        'CROP': {'ROW': 221, 'COL': 181},
        'RESIZE': 0.5
    },

    'THUMBNAIL': {
        'SAVE': True,
        'DIR': 'analysis_proper/thumbnails/',
        'EXT': ['', '.jpg', '.png']
    }
}
