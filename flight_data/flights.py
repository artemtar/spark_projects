import csv
from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from pyspark import SparkContext

def split(line):
    return map


def remHeader(row):
    return "Description" not in row

airports = sc.textFile(airports_path).filter(remHeader).map(split)