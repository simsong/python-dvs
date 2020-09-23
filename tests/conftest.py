import sys
import warnings

sys.path.append("/mnt2/gits/dasexperimental-www/python/")


try:
    import pymysql
except ModuleNotFoundError as e:
    warnings.warn("This test requires pymysql")
