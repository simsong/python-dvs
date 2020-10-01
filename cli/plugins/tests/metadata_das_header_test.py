import sys
import os
import os.path
import warnings

#from os.path import basename,dirname,abspath

#sys.path.append(dirname(dirname(abspath(__file__))))

DATAFILE = os.path.join( os.path.dirname(__file__), "das_header.txt")

from ..metadata_das_header  import *

def test_metadata_das_header():
    with open( DATAFILE, "rb") as f:
        buf = f.read(65536)
        obj = metadata_das_header(buf)
        assert obj['US Git Repo python_dvs']=='e7b2a6d3502216543b48be5570110f087792725f'
