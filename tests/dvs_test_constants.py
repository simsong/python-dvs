import os
import os.path
from os.path import dirname, basename, abspath

DVS_DEMO_FILE = 'dvs_demo.txt'
DVS_DEMO_PATH = os.path.join(dirname(__file__), DVS_DEMO_FILE)
S3LOC1 = os.environ['DAS_S3ROOT'] + '/tmp/demofile1.txt'
