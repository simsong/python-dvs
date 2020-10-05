#!/usr/bin/env python3

"""
dvs plugin reads an input file and returns a dictionary of metadata
"""

import sys
import os
import warnings

MAXREAD = 65536

def process_header(ret, line):
    assert line[0]=='#'
    if 'Git Repo Info' in line:
        (header,data) = line.split(':',1)
        header=header.replace('# ','').replace(' Info','')+' '
        for commit in data.split('|'):
            words = commit.split()
            if words[1]=='commit':
                ret[header+words[0]] = words[2]


def metadata_das_header(buf):
    ret = {}
    inheader = True
    lines = buf.decode('utf-8').split("\n")
    for line in lines:
        if inheader:
            if line[0]!='#':
                inheader = False
            else:
                process_header(ret, line)
                continue
    return ret
