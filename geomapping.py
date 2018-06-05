#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 14:07:13 2018
geodata to zipcode mapping in csv
@author: clara
"""

import urllib.request
import zipfile
import os
import fnmatch
import shutil
import csv
import json
import numpy as np
import re
import time
import math
from threading import Lock, Thread
from pathlib import Path
import pandas as pd


def get_zip_code_from_geo(lat, lng, apikey):

        zip_code = None

        contents = urllib.request.urlopen(
            "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + lat + "," + lng + "&sensor=false&key=" + apikey)
        j = json.load(contents)

        if j['status'] == "OVER_QUERY_LIMIT":
            return -2

        if len(j['results']) == 0:
            return -3

        if j['status'] != "ZERO_RESULTS":
            components = j['results'][0]['address_components']

            for comp in components:
                if comp['types'][0] == 'postal_code':
                    zip_code = comp['long_name']
                    break

        return zip_code
    
def main():
    reader = csv.reader(open("geomapping.csv", "r"), delimiter=",")
    x = list(reader)
    result = np.array(x).astype(str)
    
    print("reading")
    
    for i in range(1,result.shape[0]):
        print(i)
        result[i][3] = get_zip_code_from_geo(result[i][1],result[i][2],"AIzaSyAojtE1GHYx1HvXSaMuK98RkeboisXL954")
        print(result[i][3])
    df = pd.DataFrame(result)
    df.to_csv("geomappingnew.csv", header=None, index=None)
    
    print("done")
    
if __name__ == "__main__":
    main()