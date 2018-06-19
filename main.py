import urllib.request
import zipfile
import os
import fnmatch
import shutil
import sys
import csv
import json
import numpy as np
import re
import time
import math
import threading
from threading import Lock, Thread
from pathlib import Path

current_milli_time = lambda: int(round(time.time() * 1000))


class TempContainer:
    id = None
    mid = None

    def __init__(self, id, mid):
        self.id = id
        self.mid = mid


class Station:
    id = None
    recording_mid = None
    recording_start = None
    recording_end = None
    height = None
    latitude = None
    longitude = None
    name = None
    state = None
    zip_code = None

    def __init__(self, id, recording_start, recording_end, recording_mid, height, latitude, longitude, name, state):
        self.id = id
        self.recording_start = recording_start
        self.recording_end = recording_end
        self.height = height
        self.recording_mid = recording_mid
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
    zip_flag = 0
    NULL_TYPE = np.NaN

    thread_count = 10
    station_count = 0
    runs = 0

    progress = 0
    progress_end = 0
    start_time = None

    file_prefix = "tageswerte_KL_"
    file_suffix = "_akt.zip"
    file_url = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/"
    file_url_historical = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/historical/"
    file_suffix_historical = "_hist.zip"
    station_list = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"

    def get_zip_code_from_csv(self, station_id):
        station_id = int(station_id)
        station_id = str(station_id)

        reader = csv.reader(open("geo_to_plz.csv", "r"), delimiter=",")
        x = list(reader)
        result = np.array(x).astype(str)

        for i in range(1,result.shape[0]):
            if result[i][0] == station_id:
                return result[i][3]
        return -1


    def get_zip_code_from_geo(self, lat, lng, apikey):

        zip_code = None

        contents = urllib.request.urlopen(
            "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + str(lat) + "," + str(
                lng) + "&sensor=false&key=" + apikey)
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

    def get_station_by_id(self, id, stations):
        for station in stations:
            if station.id == id:
                return station
        return None

    # saves a list of cleaned weather data as csv
    def get_weather_data(self, file_name, zip_flag):

        self.start_time = current_milli_time();
        self.zip_flag = zip_flag
        start_time_glob = current_milli_time()

        print("\n## Get stations ##")
        start_time = current_milli_time()
        self.stations = dwd.get_stations()
        print("[runtime: " + str(current_milli_time() - start_time) + " ms]")

        print("\n\n## Get weather data ##")
        station_data = []
        start_time = current_milli_time()

        interval_len = math.ceil(len(self.stations) / self.thread_count)

        threads = [];

        for i in range(self.thread_count):
            if i == self.thread_count - 1:
                end = len(self.stations) - 1
            else:
                end = (1 + i) * interval_len

            if len(self.stations) > i * interval_len + 1:
                new_thread = Thread(target=self.get_station_data_from, args=(station_data, i * interval_len, end,i))
                threads.append(new_thread)
                new_thread.setDaemon(True)
                new_thread.start()

        for i in range(len(threads)):
            threads[i].join()



        return current_milli_time() - start_time_glob

    #

    def write_to_file(self, recent_data, hist_data):
        file = None

        recent_file = open("out_recent.csv", 'a')
        hist_file = open("out_historical.csv", 'a')

        for data in recent_data:
            recent_file.write(str(data.station_id) + ';' + str(data.station_name) + ';' + str(
            data.station_zip_code) + ';' + str(data.mess_datum) + ';' + str(data.qn_3) + ';' + str(
            data.fx) + ';' + str(data.fm) + ';' + str(data.qn_4) + ';' + str(data.rsk) + ';' + str(
            data.rskf) + ';' + str(data.sdk) + ';' + str(data.shk_tag) + ';' + str(data.nm) + ';' + str(
            data.vpm) + ';' + str(data.pm) + ';' + str(data.tmk) + ';' + str(data.upm) + ';' + str(
            data.txk) + ';' + str(data.tnk) + ';' + str(data.tgk) + ';' + str(data.eor) + "\n")

        recent_file.close()

        for data in hist_data:
            hist_file.write(str(data.station_id) + ';' + str(data.station_name) + ';' + str(
            data.station_zip_code) + ';' + str(data.mess_datum) + ';' + str(data.qn_3) + ';' + str(
            data.fx) + ';' + str(data.fm) + ';' + str(data.qn_4) + ';' + str(data.rsk) + ';' + str(
            data.rskf) + ';' + str(data.sdk) + ';' + str(data.shk_tag) + ';' + str(data.nm) + ';' + str(
            data.vpm) + ';' + str(data.pm) + ';' + str(data.tmk) + ';' + str(data.upm) + ';' + str(
            data.txk) + ';' + str(data.tnk) + ';' + str(data.tgk) + ';' + str(data.eor) + "\n")

        hist_file.close()

    def get_active_station_by_id(self, active_stations, ida):
        for x in range(len(active_stations)):

            if active_stations[x].id == ida:
                return active_stations[x]

        return None

    def get_stations_from(self, lines, active_stations, start, end):

        # index ab 2!!!!
        apikeylist = ["AIzaSyBJ1HpXkBekg9Ek553aKSILi-d-q8RlFO8",
                      "AIzaSyDOu4NU_6awk4X08-JmN7yx70U-JclaRic",
                      "AIzaSyAojtE1GHYx1HvXSaMuK98RkeboisXL954"]
        keyindex = 0

        for x in range(start, end):

            if x == 0 or x == 1:
                continue

            line = re.sub(' +', ';', lines[x])

            line = line.split(';')

            container = self.get_active_station_by_id(active_stations, line[0])

            if (container == None):
                continue

            new_station = Station(line[0], line[1], line[2], container.mid, line[3], line[4], line[5], line[6], line[7])

            if self.get_active_station_by_id(active_stations, new_station.id) != None:

                plz = self.get_zip_code_from_csv(new_station.id)
                if plz != -1:
                    new_station.set_zip_code(plz)
                if plz == -1 and self.zip_flag == 1:
                    zipc = self.get_zip_code_from_geo(new_station.latitude, new_station.longitude,
                                                      apikeylist[keyindex]);

                    while (zipc == -2 and keyindex < len(apikeylist) - 1):
                        keyindex += 1
                        zipc = self.get_zip_code_from_geo(new_station.latitude, new_station.longitude,
                                                          apikeylist[keyindex]);

                    if zipc == -2:
                        error_text = "-> error: query limit for all keys reached"
                        print(error_text)
                    elif zipc == -3:
                        error_text = " -> something went wrong"
                        print(error_text)
                    else:
                        new_station.set_zip_code(zipc)

                self.stations.append(new_station)
                self.station_count += 1

    def get_station_data_from(self, station_data, start, end,ident):
        for i in range(start, end):
        	dwd.get_station_data(self.stations[i], ident)


    def concatenate_lists(self, station_data):
        completelist = []
        for station in station_data:
            completelist += station
        return completelist

    def get_active_stations(self):
        req = urllib.request.Request('ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/')
        req2 = urllib.request.Request('ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/historical/')

        with urllib.request.urlopen(req) as response:
            html = str(response.read())

        with urllib.request.urlopen(req2) as response2:
            html2 = str(response2.read())

        active_stations = []

        historic_ids = []
        historic_mids = []

        historic_stations = []

        for item in html.split("_akt.zip"):
            if "tageswerte_KL_" in item:
                id = item[item.find("tageswerte_KL_") + len("tageswerte_KL_"):]
                tc = TempContainer(id, None)
                active_stations.append(tc)

        for item in html2.split("_hist.zip"):
            if "tageswerte_KL_" in item:
                raw = item[item.find("tageswerte_KL_") + len("tageswerte_KL_"):]
                id = raw[:5]
                mid = raw[15:23]
                historic_ids.append(id)
                historic_mids.append(mid)

        for i in range(len(active_stations)):
            mid = historic_mids[historic_ids.index(active_stations[i].id)]
            active_stations[i].mid = mid

        return active_stations

    def get_stations(self):
        active_stations = self.get_active_stations()

        urllib.request.urlretrieve(self.station_list, "temp")

        with open("temp", 'r', encoding='cp1252') as f:
            lines = f.readlines()

            stations = []

            interval_len = math.ceil(len(lines) / self.thread_count)

            threads = [];

            for i in range(self.thread_count):
                if i == self.thread_count - 1:
                    end = len(lines) - 1
                else:
                    end = (1 + i) * interval_len

                if len(lines) > i * interval_len + 1:
                    new_thread = Thread(target=self.get_stations_from,
                                        args=(lines, active_stations, i * interval_len, end))
                    threads.append(new_thread)
                    new_thread.start()

            for i in range(len(threads)):
                threads[i].join()

            os.remove("temp")

            print("-> Found " + str(len(self.stations)) + " valid stations <-")
            self.progress_end = len(self.stations)

            return self.stations






    def get_station_data(self, station, t_id):

        local_file = "station_" + station.id
        local_file_historical = "station_" + station.id + "_historical"

        recent_data = []
        hist_data = []

        historicalValid = False

        time = current_milli_time()

        # Name der Recent-Files: tageswerte_KL_00044_akt.zip
        try:
        	urllib.request.urlretrieve(self.file_url + self.file_prefix + station.id + self.file_suffix,local_file + ".zip")
        except:
        	print("zZ ",end='')
        	time.sleep(1000)
        	get_station_data(self, station)
        	return


        time = current_milli_time()

        # Name der Historical-Files: tageswerte_KL_00001_19370101_19860630_hist.zip
        try:
          urllib.request.urlretrieve(
              self.file_url_historical + self.file_prefix + station.id + "_" + station.recording_start + "_" + station.recording_mid + self.file_suffix_historical,
              local_file_historical + ".zip")
          historicalValid = True
        except:
          print("I expected some data in the historical zip, but there was none!")





        zip_ref = zipfile.ZipFile(local_file + ".zip", 'r')
        zip_ref.extractall(local_file)
        zip_ref.close()
        os.remove(local_file + ".zip")


        if(historicalValid):
            zip_ref2 = zipfile.ZipFile(local_file_historical + ".zip", 'r')
            zip_ref2.extractall(local_file_historical)
            zip_ref2.close()
            os.remove(local_file_historical + ".zip")

        for file in os.listdir(local_file):
            if fnmatch.fnmatch(file, "produkt_klima_tag*"):
                os.rename(local_file + "/" + file, local_file + ".csv")

        shutil.rmtree(local_file)


        if(historicalValid):
            for file in os.listdir(local_file_historical):
                if fnmatch.fnmatch(file, "produkt_klima_tag*"):
                    os.rename(local_file_historical + "/" + file, local_file_historical + ".csv")

            shutil.rmtree(local_file_historical)






        with open(local_file + ".csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            first_row = True
            current_station = None

            i = 0
            for row in readCSV:
                if i >= 1:
                    row = self.parse(row);
                    current_data = MeasuredData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                                row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16],
                                                row[17], row[18])

                    current_data.set_station_data(station.name, station.zip_code)

                    recent_data.append(current_data)

                i += 1


        time = current_milli_time()

        if(historicalValid):

            with open(local_file_historical + ".csv") as csvfile:
                readCSV = csv.reader(csvfile, delimiter=';')
                first_row = True
                current_station = None

                i = 0
                for row in readCSV:
                    if i >= 1:
                        row = self.parse(row);
                        current_data = MeasuredData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                                    row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16],
                                                    row[17], row[18])

                        current_data.set_station_data(station.name, station.zip_code)



                        for oldrow in recent_data:
                            if (oldrow.mess_datum == row[1]):
                                break
                            else:
                                hist_data.append(current_data)
                                break
                    i += 1
            os.remove(local_file_historical + ".csv")

        self.progress+=1
        self.update_progress(self.progress/self.progress_end)
        os.remove(local_file + ".csv")
        self.write_to_file(recent_data,hist_data)




    def update_progress(self, progress):


        time_consumed = current_milli_time() - self.start_time

        estimated_time = (time_consumed / progress)-time_consumed

        sys.stdout.write("[")

        for i in range(0, 100):
            if(int(progress*100) < i):
                sys.stdout.write(" ")
            elif(int(progress*100) == i):
                if(progress == 0.99):
                    sys.stdout.write("(100%)=")
                else:
                    sys.stdout.write("(" +str(int(progress*100)) + "% | " + str(int(estimated_time/1000)) + "sec)=>")
            else:
                sys.stdout.write("=")

        sys.stdout.write("]" + "\n" + "\r")

        sys.stdout.flush()


    def get_zip_code(lat, lon):
        zip_code = 0
        return zip_code

    def intoCSV(self, list, f_name):
        file = open(f_name, 'w')

        file.write(
            "STATION_ID;STATION_NAME;STATION_ZIP;MESS_DATUM;Qualität ff;Max Wind;D-Windgeschw.;Qualität ff;NS-Menge;NS-Art;Sonnenst.;Schneehöhe;Bedeckung;Dampfdruck;Luftdruck;D-Temp;Rel. Feuchte;Max Temp.;Min Temp.;Boden Min Temp.;eor" + "\n")

        for i in range(len(list)):
            file.write(str(list[i].station_id) + ';' + str(list[i].station_name) + ';' + str(
                list[i].station_zip_code) + ';' + str(list[i].mess_datum) + ';' + str(list[i].qn_3) + ';' + str(
                list[i].fx) + ';' + str(list[i].fm) + ';' + str(list[i].qn_4) + ';' + str(list[i].rsk) + ';' + str(
                list[i].rskf) + ';' + str(list[i].sdk) + ';' + str(list[i].shk_tag) + ';' + str(list[i].nm) + ';' + str(
                list[i].vpm) + ';' + str(list[i].pm) + ';' + str(list[i].tmk) + ';' + str(list[i].upm) + ';' + str(
                list[i].txk) + ';' + str(list[i].tnk) + ';' + str(list[i].tgk) + ';' + str(list[i].eor) + "\n")

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
file_name = "kannWeg"
dwd.thread_count = 10
zip_flag = "no"

if zip_flag == 'yes':
    zip_flag = 1
else:
    zip_flag = 0

dwd.get_weather_data(file_name, zip_flag)  # für alle: -1
