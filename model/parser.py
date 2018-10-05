"""
Parsing a document (xml, jpg, tiff) to store it into MongoDB
"""
import os
import numpy as np
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
from PIL import Image

class Parser(object):
    def __init__(self, config):
        self.config = config

    def _get_value(self, x):
        if isinstance(x, list):
            return x[-1]
        return x

    def _get_value_by_key(self, doc, key, default_value):
        """Get value from a dictionary associated with a given key"""
        if key in doc:
            val = doc[key]
            if not isinstance(default_value, str):
                val = float(val)
                if np.isnan(val) or np.isinf(val):
                    val = float(0)
            else:
                # assuming val contains name with a path.
                # only use the name.
                val = self._get_value(val.split('/'))
            return val
        return default_value

    def _add_protocol(self, pr_name, pr_time, val, doc):
        if pr_name in doc:
            if pr_time > doc[pr_name]['time']:
                val['time'] = pr_time
                doc[pr_name] = {'data': val, 'time': pr_time}
        else:
            doc[pr_name] = {'data': val, 'time': pr_time}
        return doc

    def xml_to_doc(self, filename, sample_name=None, project_name=None):
        """
        Parsing xml document.

        Args:
            filename: filename with full path
            sample_name: sample name (a.k.a group name)
                This is used to grouping a lot of results in a folder.
        Returns:
            Dictionary object if there are no errros; otherwise None.
        """
        try:
            tree = ET.parse(filename)
            doc = dict()

            root = tree.getroot()
            root_att = root.attrib

            # in a xml file, the root attribute looks like
            # <DataFile name="path/to/file/item_name.tiff"> ... </DataFile>
            # Here, we extract the item_name and used them to identify a data
            #  point over all DBs (i.e. assuming it is unique over all data)
            item_name = self._get_value_by_key(
                root_att,
                self.config['ROOTID'],
                'unknown'
            )
            if item_name == 'unknown': return None
            item_name = os.path.splitext(item_name)[0]

            if sample_name is None:
                for sep in self.config['SAMPLE_SPLIT']:
                    sample_name = sample_name.split(sep)[0]
            doc['item'] = item_name
            doc['sample'] = sample_name

            # additional, grouping information
            # typically, project contains one or more samples.
            # path is directory where the file resides
            doc['project'] = project_name
            doc['path'] = os.path.split(filename)[0]

            # Loop over all protocols
            for protocol in root:
                pr_att = protocol.attrib

                # fetch protocal name and time
                # <protocol name="metadata_extract" ...> ... </protocol>
                pr_name = self._get_value_by_key(
                    pr_att,
                    self.config['PID'],
                    'unknown'
                )
                # in default config, time is when this xml file is saved.
                pr_time = self._get_value_by_key(
                    pr_att,
                    self.config['TIMESTAMP'],
                    0
                )
                if pr_name == 'unknown':
                    continue

                # special case for thumbnails protocol
                # we will treat thumbnails (jpg) in different routine.
                if pr_name == 'thumbnails':
                    continue

                pr_dict = dict()
                for experiment in protocol:
                    ex_att = experiment.attrib
                    # typically, it looks like
                    # <result name="theta" value="0.11" />
                    ex_name = self._get_value_by_key(
                        ex_att,
                        self.config['RID'],
                        'unknown'
                    )
                    if ex_name == 'unknown': continue
                    if ex_name in self.config['R_EXCLUDE']: continue

                    # although, we accept string value here, it can cause an
                    # error later. So, it is safe to exclude all fields that
                    # belongs to string type in the config file (R_EXCLUDE).
                    default_value = 0
                    if ex_name in self.config['R_STRING']:
                        default_value = '0'
                    ex_value = self._get_value_by_key(
                        ex_att,
                        self.config['RVAL'],
                        default_value
                    )

                    pr_dict[ex_name] = ex_value
                # we will only keep the latest one based on pr_time, if there
                #  is duplicated protocols.
                self._add_protocol(pr_name, pr_time, pr_dict, doc)
        except ParseError:
            print('XML ParseError: ', filename)
            doc = None
        return doc

    def tiff_to_doc(self, filename, sample_name=None, project_name=None):
        """parsing a tiff file"""
        try:
            im = Image.open(filename)
            imarr = np.array(im)
            dim = imarr.shape

            tiff_doc = dict()
            tiff_doc['data'] = imarr
            tiff_doc['width'] = int(dim[1])
            tiff_doc['height'] = int(dim[0])
            tiff_doc['channel'] = int(1)
            tiff_doc['min'] = float(imarr.min())
            tiff_doc['max'] = float(imarr.max())

            # store path in the nested dictionary to avoid conflicting with
            # a path of correponding xml file path
            tiff_doc['path'] = os.path.split(filename)[0]


            item = os.path.splitext(filename)[0]
            item = item.split('/')[-1]
            doc = dict()
            # here, assume that item name is same as the item name of
            # corresponding xml file and also for project name and
            # sample_name although those arguments are not requiremnts.
            doc['item'] = item
            doc['tiff'] = tiff_doc
            if sample_name is not None:
                doc['sample'] = sample_name
            if project_name is not None:
                doc['project'] = project_name

            return doc
        except:
            return None

    def jpg_to_doc(self, filename, sample_name=None, project_name=None):
        """parsing a jpg file"""
        try:
            im = Image.open(filename)
            imarr = np.array(im)
            dim = imarr.shape

            jpg_doc = dict()
            jpg_doc['data'] = imarr
            jpg_doc['width'] = int(dim[1])
            jpg_doc['height'] = int(dim[0])
            jpg_doc['channel'] = int(dim[2])
            jpg_doc['min'] = int(0)
            jpg_doc['max'] = int(255)

            # store path in the nested dictionary to avoid conflicting with
            # a path of correponding xml file path
            jpg_doc['path'] = os.path.split(filename)[0]

            item = os.path.splitext(filename)[0]
            item = item.split('/')[-1]
            doc = dict()
            # here, assume that item name is same as the item name of
            # corresponding xml file and also for project name and
            # sample_name although those arguments are not requiremnts.
            doc['item'] = item
            doc['jpg'] = jpg_doc
            if sample_name is not None:
                doc['sample'] = sample_name
            if project_name is not None:
                doc['project'] = project_name
            return doc
        except:
            return None

    def run(self, path, kind, sample_name, project_name):
        if kind == 'xml':
            return self.xml_to_doc(path, sample_name, project_name)
        elif kind == 'jpg':
            return self.jpg_to_doc(path, sample_name, project_name)
        elif kind == 'tiff':
            return self.tiff_to_doc(path, sample_name, project_name)
        else:
            print('[PARSER] Unsupported file type: {}'.format(kind))
            return None


if __name__ == '__main__':
    from config import CONFIG
    import pprint

    parser = Parser(config=CONFIG['XML'])
    pp = pprint.PrettyPrinter(indent=4)

    print('Test parsing a xml file')
    test_files = '/Users/scott/Desktop/data/saxs/analysis_proper/results/' \
                 'C67_GD2-69-6_th0.110_1929.1s_T200.006C_5.00s_61288_saxs.xml'
    doc = parser.xml_to_doc(test_files, 'test_sample', 'test_project')
    if doc is None:
        print('Error occures while parsing xml document.')
    else:
        pp.pprint(doc)

    print('\nTest parsing a jpg file')
    test_files = '/Users/scott/Desktop/data/saxs/analysis_proper/thumbnails' \
                 '/C67_GD2-69-6_th0.110_1929.1s_T200.006C_5.00s_61288_saxs.jpg'
    doc = parser.jpg_to_doc(test_files, 'test_sample', 'test_project')
    if doc is None:
        print('Error occures while parsing jpg document.')
    else:
        pp.pprint(doc)

    print('\nTest parsing a tiff file')
    test_files = '/Users/scott/Desktop/data/saxs/tiff' \
                 '/C67_GD2-69-6_th0.110_1929.1s_T200.006C_5.00s_61288_saxs.tiff'
    doc = parser.tiff_to_doc(test_files, 'test_sample', 'test_project')
    if doc is None:
        print('Error occurres while parsing tiff document.')
    else:
        pp.pprint(doc)














