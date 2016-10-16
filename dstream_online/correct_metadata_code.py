#! /usr/bin/env python
## @namespace dstream_online.get_metadata
#  @ingroup dstream_online
#  @brief Defines a project dstream_online.get_metadata
#  @author echurch, yuntse

# python include
import time, os, shutil, sys, gc
# pub_dbi package include
import datetime, json
import pdb
import subprocess
import glob
import samweb_cli

try:
    samweb = samweb_cli.SAMWebClient(experiment="uboone")
except:
    raise Exception('Not able to open up the SAMWeb Connection')

if len(sys.argv)<2 :
    print("\nMissing a query string to search for files!!")
    print("Usage: correct_metadata.py <file_query>\n")
    exit(1)

file_query=str(sys.argv[1])

if len(file_query)<2:
    print("\nError: file_query needs to have have something to if otherwise no files will be found!!!\n")
    exit(1)

list_o_files = samweb.listFiles(file_query)

if len(list_o_files)> 1000:
    print("\nError: Holy Cow!!! That's way too many files to process at one time. it's like %d files!!!\n" % len(list_o_files))
    exit(1)

for index_o_files in xrange(len(list_o_files)):
    
    in_file = list_o_files[index_o_files]
    md = samweb.getMetadata(filenameorid=in_file)
    in_file_split = in_file.split('-')
    if in_file_split[0]=='PhysicsRun':
        run_type='physics'
    elif in_file_split[0]=='NoiseRun':
        run_type='noise'
    elif in_file_split[0]=='CalibrationRun':
        run_type='calibration'
    elif in_file_split[0]=='PMTCalibrationRun':
        run_type='pmtcalibration'
    elif in_file_split[0]=='TPCCalibrationRun':
        run_type='tpccalibration'
    elif in_file_split[0]=='LaserCalibrationRun':
        run_type='lasercalibration'
    elif in_file_split[0]=='BeamOffRun':
        run_type='beamoff'
    elif in_file_split[0]=='BeamOff':
        run_type='beamoff'
    elif in_file_split[0]=='TestRun':
        run_type='test'
    else:
        run_type='unknown'

    for index_o_runs in xrange(len(md['runs'])):
        md['runs'][index_o_runs][2]=run_type

    new_md={'runs': md['runs']}
    if new_md['runs']!=0:
        print(new_md)
        samweb.modifyFileMetadata(in_file,md=new_md)

print("Finished processing files for this query: %s" % file_query)

exit(0)
