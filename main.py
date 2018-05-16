import urllib.request
import zipfile
import os
import fnmatch
import shutil
import csv
import re
from pprint import pprint
import json
import pandas as pd
import numpy as np
import re
import sys

class Station:
    id = None
    recording_start = None
    recording_end = None
    height = None
    latitude = None
    longitude = None
    name = None
    state = None
    zip_code = None

    def __init__(self, id, recording_start, recording_end, height, latitude, longitude, name, state):
        self.id = id
        self.recording_start = recording_start
        self.recording_end = recording_end
        self.height = height
        self.latitude = latitude
        self.longitude = longitude
        self.name = name
        self.state = state

    def set_zip_code(self, zip_code):
      self.zip_code = zip_code


class MeasuredData:
    station_name = None
    station_zip_code = None

    station_id = None
    mess_datum = None
    qn_3 = None
    fx = None
    fm = None
    qn_4 = None
    rsk = None
    rskf = None
    sdk = None
    shk_tag = None
    nm = None
    vpm = None
    pm = None
    tmk = None
    upm = None
    txk = None
    tnk = None
    tgk = None
    eor = None

    def set_station_data(self, name, zip_code):
        self.station_name = name
        self.station_zip_code = zip_code

    def __init__(self, stations_id, mess_datum, qn_3, fx, fm, qn_4, rsk, rskf, sdk, shk_tag, nm, vpm, pm, tmk, upm, txk,
                 tnk, tgk, eor):
        self.station_id = stations_id
        self.mess_datum = mess_datum
        self.qn_3
        self.fx = fx
        self.fm = fm
        self.qn_4 = qn_4
        self.rsk = rsk
        self.rskf = rskf
        self.sdk = sdk
        self.shk_tag = shk_tag
        self.nm = nm
        self.vpm = vpm
        self.pm = pm
        self.tmk = tmk
        self.upm = upm
        self.txk = txk
        self.tnk = tnk
        self.tgk = tgk
        self.eor = eor


class DWD:
    stations = None;

    file_prefix = "tageswerte_KL_"
    file_suffix = "_akt.zip"
    file_url = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/"
    station_list = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"


    def get_zip_code_from_geo(self, lat, lng):
        apikey = "AIzaSyBJ1HpXkBekg9Ek553aKSILi-d-q8RlFO8"

        zip_code = None

        contents = urllib.request.urlopen(
            "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + str(lat) + "," + str(lng) + "&sensor=false&key=" + apikey)
        j = json.load(contents)


        
        if j['status'] != "ZERO_RESULTS":
          components = j['results'][0]['address_components']

          for comp in components:
            if comp['types'][0] == 'postal_code':
              zip_code = comp['long_name']
              break

     
        return zip_code

    def get_station_by_id(self, id, stations):
        for station in stations:
            if station.id == id:
                return station
        return None

    # saves a list of cleaned weather data as csv
    def get_weather_data(self, max_stations, file_name):

        print("\n## Get stations ##")
        self.stations = dwd.get_stations(max_stations)

        station_data = []


        print("\n\n## Get weather data ##")

        for station in self.stations:
            print("fetch data from station: " + station.id, end='')
            station_data.append(dwd.get_station_data(station))

        print("-> weather data fetched <-")

        print("\n\n## Concatenate lists ##")
        print("concatenate", end='')
        full_list = self.concatenate_lists(station_data)
        print(" -> done")


        print("\n\n## Safe CSV ##")
        print("export", end='')
        self.intoCSV(full_list, file_name)
        print("-> done")
        print("-> exported "  + str(len(full_list)) + " datasets from "+ str(len(self.stations)) + " stations <-")
        

        print("\n\n")
        return True

    #

    def concatenate_lists(self, station_data):
        completelist = []
        for station in station_data:
            completelist += station
        return completelist

    def get_stations(self, max_stations):

        station_count = 0

        req = urllib.request.Request('ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/')
        with urllib.request.urlopen(req) as response:
          html = str(response.read())

        active_stations = []

        for item in html.split("_akt.zip"):
          if "tageswerte_KL_" in item:
            active_stations.append(item[ item.find("tageswerte_KL_")+len("tageswerte_KL_") : ])


        urllib.request.urlretrieve(self.station_list, "temp")



        with open("temp", 'r', encoding='cp1252') as f:
            lines = f.readlines()

            stations = []

            

            for x in range(2, len(lines)):
                print("Get Station " + str(x-1) + "/" + str(len(lines)-1), end='')

                lines[x] = re.sub(' +', ';', lines[x])
                line = lines[x].split(';')

                new_station = Station(line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7])

                if new_station.id in active_stations:
                  print(" -> get zip code")
                  new_station.set_zip_code(self.get_zip_code_from_geo(new_station.latitude, new_station.longitude))
                  stations.append(new_station)
                  station_count+=1
                else:
                  print(" -> invalid")

                if(max_stations != -1 and station_count >= max_stations):
                  break

            os.remove("temp")

            print("-> Found " + str(len(stations)) + " valid stations <-")

            return stations

    def get_station_data(self, station):
        local_file = "station_" + station.id

        data = []

        try:
            urllib.request.urlretrieve(self.file_url + self.file_prefix + station.id + self.file_suffix,
                                       local_file + ".zip")
        except Exception:
            print("->station data doesn't exist")
            return data

        zip_ref = zipfile.ZipFile(local_file + ".zip", 'r')
        zip_ref.extractall(local_file)
        zip_ref.close()
        os.remove(local_file + ".zip")

        for file in os.listdir(local_file):
            if fnmatch.fnmatch(file, "produkt_klima_tag*"):
                os.rename(local_file + "/" + file, local_file + ".csv")

        shutil.rmtree(local_file)

        with open(local_file + ".csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            first_row = True
            current_station = None

            i = 0
            for row in readCSV:
              if i>=1:
                current_data = MeasuredData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                            row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16],
                                            row[17], row[18])
                

                current_data.set_station_data(station.name, station.zip_code)

                data.append(current_data)
              i+=1

        os.remove(local_file + ".csv")

        print(" -> ok")

        return data

    def get_zip_code(lat, lon):
        zip_code = 0
        return zip_code

    def intoCSV(self, list, f_name):
        array = ["STATION_ID", "STATION_NAME", "STATION_ZIP", "MESS_DATUM","QN_3","FX","FM","QN_4","RSK","RSKF","SDK","SHK_TAG","NM","VPM","PM","TMK","UPM","TXK","TNK","TGK", "eor"]

        
        for i in range(list.__len__()):
            stationd = list[i]
            array = np.vstack((array,
                              [stationd.station_id, stationd.station_name, stationd.station_zip_code, stationd.mess_datum, stationd.qn_3, stationd.fx, stationd.fm,
                               stationd.qn_4, stationd.rsk, stationd.rskf, stationd.sdk, stationd.shk_tag, stationd.nm,
                               stationd.vpm, stationd.pm, stationd.tmk, stationd.upm, stationd.txk, stationd.tnk,
                               stationd.tgk, stationd.eor]))

        for i in range(array.shape[1]):
            if array[0][i] == "FM":
                array[0][i] = "D-Windgeschw."
            elif array[0][i] == "RSK":
                array[0][i] = "NS-Menge"
            elif array[0][i] == "RSKF":
                array[0][i] = "NS-Art"
            elif array[0][i] == "TMK":
                array[0][i] = "D-Temp"
            elif array[0][i] == "STATION_ID":
                array[0][i] = "STATION_ID"

            elif array[0][i] == "STATION_NAME":
                array[0][i] = "STATION_NAME"

            elif array[0][i] == "STATION_ZIP":
                array[0][i] = "STATION_ZIP"
            elif array[0][i] == "QN_3":
                array[0][i] = "Qualität ff"
            elif array[0][i] == "FX":
                array[0][i] = "Max Wind"
            elif array[0][i] == "QN_4":
                array[0][i] = "Qualität ff"
            elif array[0][i] == "SDK":
                array[0][i] = "Sonnenst."
            elif array[0][i] == "SHK_TAG":
                array[0][i] = "Schneehöhe"
            elif array[0][i] == "NM":
                array[0][i] = "Bedeckung"
            elif array[0][i] == "VPM":
                array[0][i] = "Dampfdruck"
            elif array[0][i] == "PM":
                array[0][i] = "Luftdruck"
            elif array[0][i] == "UPM":
                array[0][i] = "Rel. Feuchte"
            elif array[0][i] == "TXK":
                array[0][i] = "Max Temp."
            elif array[0][i] == "TNK":
                array[0][i] = "Min Temp."
            elif array[0][i] == "TGK":
                array[0][i] = "Boden Min Temp."

        for j in range(array.shape[0]):
            if array[j][7] == "   0":
                array[j][7] = "kein NS"
            elif array[j][7] == "1":
                array[j][7] = "Regen"
            elif array[j][7] == "   4":
                array[j][7] = "Form unbekannt"
            elif array[j][7] == "   6":
                array[j][7] = "Regen"
            elif array[j][7] == "   7":
                array[j][7] = "Schnee"
            elif array[j][7] == "   8":
                array[j][7] = "Schneeregen"
            elif array[j][7] == "   9":
                array[j][7] = "Nicht feststellbar"

        for i in range(array.shape[0]):
            for j in range(array.shape[1]):
                array[i][j].__str__()

        for i in range(array.shape[0]):
            for j in range(array.shape[1]):
                if array[i][j].__str__().__contains__("-999"):
                    array[i][j] = np.NaN

        np.savetxt(f_name, array.astype(np.str), fmt="%s", delimiter=";")




dwd = DWD()

print("\n\nFetch data from DWD")
print("-----------------------")
stations = input('max. number of stations (all: -1): ')
file_name = input('output file: ')


dwd.get_weather_data(int(stations), file_name); # für alle: -1

