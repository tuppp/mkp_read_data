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
import time
from threading import Lock, Thread

current_milli_time = lambda: int(round(time.time() * 1000))


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
        self.qn_3 = qn_3
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
    stations = []

    NULL_TYPE = np.NaN

    thread_count = 10
    station_count = 0
    lock = Lock()
    runs=0

    file_prefix = "tageswerte_KL_"
    file_suffix = "_akt.zip"
    file_url = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/"
    station_list = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"


    def get_zip_code_from_geo(self, lat, lng, apikey):

        zip_code = None

        contents = urllib.request.urlopen(
            "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + str(lat) + "," + str(lng) + "&sensor=false&key=" + apikey)
        j = json.load(contents)


        if j['status'] == "OVER_QUERY_LIMIT":
          return -2



        if  len(j['results']) ==0:
          return -3

        
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

        start_time_glob = current_milli_time()

        print("\n## Get stations ##")
        start_time = current_milli_time()
        self.stations = dwd.get_stations(max_stations)
        print("[runtime: " + str(current_milli_time()-start_time) + " ms]")


       
        print("\n\n## Get weather data ##")
        station_data = []
        start_time = current_milli_time()


        interval_len = int(len(self.stations)/self.thread_count)

        threads = [];

        for i in range(self.thread_count):
          if i == self.thread_count-1:
            end = len(self.stations)-1
          else:
            end = (1+i)*interval_len

          new_thread = Thread(target=self.get_station_data_from, args=(station_data, i*interval_len, end))
          threads.append(new_thread)
          new_thread.start()


        for i in range(self.thread_count):
          threads[i].join()





        #for station in self.stations:
        #    station_data.append(dwd.get_station_data(station))




        print("-> weather data fetched <-")
        print("[runtime: " + str(current_milli_time()-start_time) + " ms]")

        print("\n\n## Concatenate lists ##")
        start_time = current_milli_time()

        print("concatenate", end='')

        full_list = self.concatenate_lists(station_data)
        print(" -> done")
        print("[runtime: " + str(current_milli_time()-start_time) + " ms]")


        print("\n\n## Safe CSV ##")
        start_time = current_milli_time()

        print("export", end='')
        self.intoCSV(full_list, file_name)
        print("-> done")
        print("[runtime: " + str(current_milli_time()-start_time) + " ms]")

        print("-> exported "  + str(len(full_list)) + " datasets from "+ str(len(self.stations)) + " stations <-")
        
        print("\n\n")

        print("[complete runtime: " + str(current_milli_time()-start_time_glob) + " ms]")

        print("\n\n")
        return current_milli_time()-start_time_glob

    #


    def get_stations_from(self,lines, active_stations,max_stations, start, end):

      #index ab 2!!!!
     apikeylist = ["AIzaSyBJ1HpXkBekg9Ek553aKSILi-d-q8RlFO8", 
                     "AIzaSyDOu4NU_6awk4X08-JmN7yx70U-JclaRic"]
     keyindex = 0

     for x in range(start, end):

        if x==0 or x==1:
          continue


        line = re.sub(' +', ';', lines[x])

        line = line.split(';')

        new_station = Station(line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7])

        print("Get Station " + new_station.id, end='')
        
        if new_station.id in active_stations:         
            print(" -> get zip code")
            zipc = self.get_zip_code_from_geo(new_station.latitude, new_station.longitude, apikeylist[keyindex]);

            while(zipc == -2):
                keyindex+=1
                zipc = self.get_zip_code_from_geo(new_station.latitude, new_station.longitude, apikeylist[keyindex]);
                if(keyindex >= len(apikeylist)):
                    break
          
            if zipc == -2:
                print("-> error: query limit for all keys reached")
            elif zipc == -3:
                print(" -> something went wrong")
            else:
                new_station.set_zip_code(zipc)
                print(" -> ok")
   
            
            if max_stations != -1:
                self.lock.acquire()

            if(max_stations != -1 and self.station_count >= max_stations):
                self.lock.release()
                break

            self.stations.append(new_station)
            self.station_count+=1

            if max_stations != -1:
                self.lock.release()
          
        else:
            print(" -> invalid")




    def get_station_data_from(self, station_data, start, end):
      for i in range(start, end):
        station_data.append(dwd.get_station_data(self.stations[i]))


    def concatenate_lists(self, station_data):
        completelist = []
        for station in station_data:
            completelist += station
        return completelist

    def get_stations(self, max_stations):

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

            interval_len = int(len(lines)/self.thread_count)

            threads = [];

            for i in range(self.thread_count):
              if i == self.thread_count-1:
                end = len(lines)-1
              else:
                end = (1+i)*interval_len

              print("from " + str(i*interval_len) + " to " + str(end))

              new_thread = Thread(target=self.get_stations_from, args=(lines,active_stations,max_stations, i*interval_len, end))
              threads.append(new_thread)
              new_thread.start()


            for i in range(self.thread_count):
              threads[i].join()

            os.remove("temp")

            print("-> Found " + str(len(self.stations)) + " valid stations <-")

            return self.stations

    def get_station_data(self, station):
        print("fetch data from station: " + station.id)
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
                row = self.parse(row);
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



    def intoCSV(self,list, f_name):
      file = open(f_name, 'w') 

      file.write("STATION_ID;STATION_NAME;STATION_ZIP;MESS_DATUM;Qualität ff;Max Wind;D-Windgeschw.;Qualität ff;NS-Menge;NS-Art;Sonnenst.;Schneehöhe;Bedeckung;Dampfdruck;Luftdruck;D-Temp;Rel. Feuchte;Max Temp.;Min Temp.;Boden Min Temp.;eor" + "\n")

      for i in range(len(list)):
        file.write(str(list[i].station_id) + ';' + str(list[i].station_name) + ';' + str(list[i].station_zip_code) + ';' + str(list[i].mess_datum) + ';' + str(list[i].qn_3) + ';' + str(list[i].fx) + ';' + str(list[i].fm) + ';' + str(list[i].qn_4) + ';' + str(list[i].rsk) + ';' + str(list[i].rskf) + ';' + str(list[i].sdk) + ';' + str(list[i].shk_tag) + ';' + str(list[i].nm) + ';' + str(list[i].vpm) + ';' + str(list[i].pm) + ';' + str(list[i].tmk) + ';' + str(list[i].upm) + ';' + str(list[i].txk) + ';' + str(list[i].tnk) + ';' +str(list[i].tgk) + ';' + str(list[i].eor) + "\n") 
      
      file.close() 


    def parse(self, row):

      for i in range(len(row)):
        row[i] = row[i].strip()
        
        if row[i] == "-999":
          row[i] = self.NULL_TYPE

      if row[7] == "0":
          row[7] = "kein NS"
      elif row[7] == "1":
          row[7] = "Regen"
      elif row[7] == "4":
          row[7] = "Form unbekannt"
      elif row[7] == "6":
          row[7] = "Regen"
      elif row[7] == "7":
          row[7] = "Schnee"
      elif row[7] == "8":
          row[7] = "Schneeregen"
      elif row[7] == "9":
          row[7] = "Nicht feststellbar"

      return row

        

dwd = DWD()

print("\n\nFetch data from DWD")
print("-----------------------")
stations = input('fetch (all | debug): ')
file_name = input('output file: ')
dwd.thread_count = int(input('threads: '))

if stations == 'all':
  stations = -1;
else:
  stations = 10;


dwd.get_weather_data(int(stations), file_name) # für alle: -1
