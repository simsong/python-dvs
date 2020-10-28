#!/usr/bin/env python3

"""
dvs batch:
Read objects specified by the batch location and load them into the database directly.
Supports offline operation.

ctools and dvs must be in the path

"""

import os
import sys
import random
import json
import warnings
import time
import boto3

from os.path import dirname, abspath, basename, realpath


MAX_OBJECT_SIZE = 4*1024*1024

POSSIBLE_DAS_DECENNIAL=dirname(dirname(dirname(dirname(realpath(__file__)))))
if basename(POSSIBLE_DAS_DECENNIAL)=='das_decennial':
    sys.path.append(os.path.join(POSSIBLE_DAS_DECENNIAL,'das_framework'))
    sys.path.append(os.path.join(POSSIBLE_DAS_DECENNIAL,'programs/python_dvs'))

sys.path.append(os.path.join(dirname(dirname(dirname(dirname( realpath(__file__))))),'bin'))

import ctools
import ctools.clogging
import dvs

from dvs.observations import get_bucket_key
from dvs.dvs_helpers import is_hexadecimal


def process_s3object(s3object):
    if s3object.size > MAX_OBJECT_SIZE:
        return
    obj = json.loads(s3object.get()['Body'].read())
    print(obj)



def process_s3path(path):
    (bucket_name,prefix) = get_bucket_key(path)
    for s3object in boto3.resource('s3').Bucket(bucket_name).objects.page_size(100).filter(Prefix=prefix):
        if is_hexadecimal( basename( s3object.key)):
            process_s3object(s3object)



if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", nargs='*', help="One or more files or directories to process")
    if ctools is not None:
        ctools.clogging.add_argument(parser,loglevel_default='WARNING')
    args = parser.parse_args()
    for path in args.path:
        if path.startswith('s3://'):
            process_s3path(path)
        else:
            process_path(path)
