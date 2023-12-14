"""
Copyright notice
  --------------------------------------------------------------------
  Copyright (C) 2020 Deltares

  This library is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this library.  If not, see <http://www.gnu.org/licenses/>.
  --------------------------------------------------------------------

This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
OpenEarthTools is an online collaboration to share and manage data and
programming tools in an open source, version controlled environment.
Sign up to recieve regular updates of this function, and to contribute
your own tools.
$Keywords: """

import requests
from urllib.parse import urlencode
import logging
import os
import traceback
import errno
import time
import sys
import glob
import json
import xml.etree.ElementTree as ET
import optparse as op
from datetime import datetime
import re
import geomet.wkt
import geojson
import zipfile
import shutil
from eodms_rapi import EODMSRAPI
from tqdm import tqdm

# Set mission specific parameters
producttype = 'GRD'

# get command line arguments - this is the name of the run file
cmdLineArgs = op.OptionParser()
cmdLineArgs.add_option('--runInfoFile', '-r', default=os.path.join(os.getcwd(), 'get_RCM.xml'))
cmdOptions, cmdArguments = cmdLineArgs.parse_args()
infofile = cmdOptions.runInfoFile
root = []


try:
    # parsing xml file
    ns = {'fews': 'http://www.wldelft.nl/fews/PI'}
    xml_input = ET.parse(infofile)
    root = xml_input.getroot()
except:
    print('ERROR: Reading INFO file - file not found. Check path to INFO file. ')
    exit()

# reading credentials from xml file
prop_find = root.findall('fews:properties', ns)
properties = {n.attrib['key']: n.attrib['value'] for n in prop_find[0]}

# reading directories
workdir = root.find('fews:workDir', ns).text
timezone = root.find('fews:timeZone', ns).text

# reading start and end time
t0_in = root.find('fews:startDateTime', ns).attrib
t0 = datetime.strptime('{} {}'.format(t0_in['date'], t0_in['time']), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d_%H%M%S')  # '%Y-%m-%d %H:%M:%S'
t1_in = root.find('fews:endDateTime', ns).attrib
t1 = datetime.strptime('{} {}'.format(t1_in['date'], t1_in['time']), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d_%H%M%S') # '%Y-%m-%d %H:%M:%S'

# start logging
loglevel = root.find('fews:logLevel', ns).text
numeric_level = getattr(logging, loglevel.upper(), None)
logger = logging.basicConfig(filename=os.path.join(workdir, 'log.txt'), filemode='w', level=numeric_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('log')

def execute_download():
    """ main function to execute the download of a specific mission """
    start_time = time.time()
    logging.debug('RCMSearch loaded')
    # Create the EODMSRAPI object
    rapi = EODMSRAPI(properties['user'], properties['secret'])

    # Add a point to the search
    geo_object = glob.glob(os.path.join(workdir, '*.geojson'))
    feat = [('intersects', geojson_to_wkt(read_geojson(geo_object[0])))]

    # Create a dictionary of query filters for the search
    filters = {'Beam Mnemonic': ('like', ['%'+properties['resolution']+'%']),
               'Product Type': ('=', producttype),
               'Polarization': ('like', ['%'+properties['polarization']+'%'])}

    # Set a date range for the search
    dates = [{"start": t0, "end": t1}]

    # Submit the search to the EODMSRAPI, specifying the Collection
    rapi.search(properties['mission'], filters=filters, features=feat, dates=dates)

    # Get the results from the search
    res = rapi.get_results('raw')
    # Print results
    rapi.print_results()
    if len(res) == 0:
        logging.info('No images found, exiting download script...')
        log2xml(os.path.join(workdir, 'log.txt'), os.path.join(workdir, 'diag.xml'))
        quit()

    params = [{"packagingFormat": "ZIP"}]
    order_res = rapi.order(res, priority="Urgent", parameters=params)
    max_attempts = int(float(properties['timeout']))

    download_res = rapi.download(order_res, create_subdir(properties['destinationDir']), wait=60, max_attempts=max_attempts)

    if len(download_res) == 0:
        print('Ordering images took longer than {} minutes. Continue...'.format(max_attempts))
        logging.warning('Ordering RCM images took longer than {} minutes. Continue...'.format(max_attempts))
        log2xml(os.path.join(workdir, 'log.txt'), os.path.join(workdir, 'diag.xml'))

    if properties['unzip'] == "True":
        logging.debug('Extracting products : ...')
        extract_all(create_subdir(properties['destinationDir']))
        logging.debug('Extracting finished.')
    else:
        logging.debug('Attribute unzip set to false, not unzipping files.')
    logging.info('Download finished in : {:.2f}s'.format(time.time() - start_time))
    log2xml(os.path.join(workdir, 'log.txt'), os.path.join(workdir, 'diag.xml'))


def extract_all(out_dir, remove=False):
    """unzips all products in the temp folder and stores it in their mission directory. optional to remove file"""
    files = sorted(glob.glob(os.path.join(out_dir, '*.zip')))
    for item in files:
        logging.debug(item)
        new_path = create_subdir(os.path.join(out_dir, os.path.splitext(item)[0]))
        with zipfile.ZipFile(item) as zf:
            for member in tqdm(zf.infolist(), desc='Extracting '):
                try:
                    zf.extract(member, path=new_path)
                except zipfile.error as e:
                    pass
        zf.close()
        if remove is True:
            os.remove(item)


def remove_html_tags(text):
    """Removes HTML tags and creates single line"""
    TAG_RE = re.compile(r'<[^>]+>')
    text_new = TAG_RE.sub('', text)
    return text_new.replace('\n', ' ').replace('-', ':')


def quit():
    sys.exit()


def error_check(properties):
    """ Check if input properties are correct and exist"""

    # check if properties exist in xml file
    for k in ['destinationDir', 'user', 'secret', 'mission', 'unzip', 'download_type', 'timeout', 'resolution', 'polarization']:
        if k not in properties.keys():
            logging.error('Property [ {} ] is not configured. Add property [ {} ] to the xml input file. '.format(k, k))
            quit()

    # check if properties have correct input
    check = {'mission': ['RCMImageProducts'],
             'download_type': ['acquisition', 'ingestion'],
             'unzip': ['True', 'False'],
             'resolution': ['Any', '100M', '50M', '30M', '16M', '5M', '3M', 'FSL', 'H', 'LNS'],
             'polarization': ['Any', 'CH CV', 'HH', 'HH HV', 'HH HV VH VV', 'HH VV', 'HV', 'VH', 'VH VV', 'VV']
             }

    for key in properties:
        if key in check:
            if properties[key] not in check[key]:
                logging.error('Wrong input to property [ {} ], has to be one of: {}'.format(key, check[key]))
                quit()

    return properties


def create_subdir(directory):
    """Helper function to create subdirectories"""
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return directory


def log2xml(logfile, xmldiag):
    """    Converts a wflow log file to a Delft-Fews XML diag file    """
    trans = {'WARNING': '2', 'ERROR': '1', 'INFO': '3', 'DEBUG': '4'}
    if os.path.exists(logfile):
        logfile = open(logfile, "r")
        xml_file = open(xmldiag, "w")
        all_lines = logfile.readlines()
        xml_file.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        xml_file.write("<Diag xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n")
        xml_file.write("xmlns=\"http://www.wldelft.nl/fews/PI\" xsi:schemaLocation=\"http://www.wldelft.nl/fews/PI ")
        xml_file.write("http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_diag.xsd\" version=\"1.2\">\n")
        for aline in all_lines:
            try:
                lineparts = aline.strip().split(" - ")
                lineparts = [l.replace('–', '').replace('"', '') for l in lineparts]
                datetime = lineparts[0].replace(',', ':').split(' ')
                line = "<line date=\"{}\" time=\"{}\" level=\"{}\" description=\"{}\"/>\n".format(datetime[0], datetime[1][0:8],
                                                                                                   trans[lineparts[2]], lineparts[3])
                xml_file.write(line)
            except:
                nothing = True
        xml_file.write("</Diag>")
        logfile.close()
        xml_file.close()


def read_geojson(geojson_file):
    """Read a GeoJSON file into a GeoJSON object.
    """
    with open(geojson_file) as f:
        return geojson.load(f)


def geojson_to_wkt(geojson_obj, feature_number=0, decimals=4):
    """Convert a GeoJSON object to Well-Known Text. Intended for use with OpenSearch queries.

    In case of FeatureCollection, only one of the features is used (the first by default).
    3D points are converted to 2D.

    Parameters
    ----------
    geojson_obj : dict
        a GeoJSON object
    feature_number : int, optional
        Feature to extract polygon from (in case of MultiPolygon
        FeatureCollection), defaults to first Feature
    decimals : int, optional
        Number of decimal figures after point to round coordinate to. Defaults to 4 (about 10
        meters).

    Returns
    -------
    polygon coordinates
        string of comma separated coordinate tuples (lon, lat) to be used by SentinelAPI
    """
    if 'coordinates' in geojson_obj:
        geometry = geojson_obj
    elif 'geometry' in geojson_obj:
        geometry = geojson_obj['geometry']
    else:
        geometry = geojson_obj['features'][feature_number]['geometry']

    def ensure_2d(geometry):
        if isinstance(geometry[0], (list, tuple)):
            return list(map(ensure_2d, geometry))
        else:
            return geometry[:2]

    def check_bounds(geometry):
        if isinstance(geometry[0], (list, tuple)):
            return list(map(check_bounds, geometry))
        else:
            if geometry[0] > 180 or geometry[0] < -180:
                raise ValueError('Longitude is out of bounds, check your JSON format or data')
            if geometry[1] > 90 or geometry[1] < -90:
                raise ValueError('Latitude is out of bounds, check your JSON format or data')

    # Discard z-coordinate, if it exists
    geometry['coordinates'] = ensure_2d(geometry['coordinates'])
    check_bounds(geometry['coordinates'])

    wkt = geomet.wkt.dumps(geometry, decimals=decimals)
    # Strip unnecessary spaces
    wkt = re.sub(r'(?<!\d) ', '', wkt)
    return wkt


if __name__ == "__main__":
    execute_download()
