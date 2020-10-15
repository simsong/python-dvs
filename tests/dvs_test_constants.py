import os
import os.path
from os.path import dirname, basename, abspath

DVS_DEMO_PATH = os.path.join(dirname( os.path.abspath(__file__)), 'dvs_demo.txt')
S3LOC1 = os.environ['DAS_S3ROOT'] + '/tmp/demofile1.txt'
