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
from socket import error as SocketError
import chardet
import pandas as pd


current_milli_time = lambda: int(round(time.time() * 1000))
onlyrecent = False
def newrecents():
    try:
        with open('recent.csv', 'r') as t1, open('out_recent.csv', 'r') as t2:
            fileone = t1.readlines()
            filetwo = t2.readlines()
            nol = len(filetwo)
            i = 1

        with open('update.csv', 'w') as outFile:
            for line in filetwo:
                print(i, "/", nol)
                i = i +1
                if line not in fileone:
                    outFile.write(line)
            outFile.flush()
        old = pd.read_csv("recent.csv", sep= ";", header= None, dtype=str)
        update = pd.read_csv("update.csv", sep= ";", header= None, dtype=str)
        glue = pd.DataFrame([])
        glue = glue.append(old)
        glue = glue.append(update)

        glue.to_csv("recent.csv",
                             header=None, index=None, sep=";")
        update.to_csv("update.csv",
                             header=["STATION_ID", "STATION_NAME", "STATION_ZIP", "MESS_DATUM", "Qualität ff", "Max Wind",
                                     "D-Windgeschw.", "Qualität ff", "NS-Menge", "NS-Art", "Sonnenst.", "Schneehöhe",
                                     "Bedeckung", "Dampfdruck", "Luftdruck", "D-Temp", "Rel. Feuchte", "Max Temp.",
                                     "Min Temp.",
                                     "Boden Min Temp.", "eor"], index=None, sep=";")
    except:
        pass


def realpatafix():
    if onlyrecent:
        rec = pd.read_csv("out_recent.csv", header=None, sep=";", dtype=str)
        glue = pd.DataFrame([])
        glue = glue.append(rec)
        glue = glue.replace(r'^\s+$', "nan", regex=True)
        glue = glue.replace(np.nan, "nan", regex=True)
        glue.to_csv("recent.csv",
                    header=["STATION_ID", "STATION_NAME", "STATION_ZIP", "MESS_DATUM", "Qualitaet ff", "Max Wind",
                            "D-Windgeschw.", "Qualitaet ff", "NS-Menge", "NS-Art", "Sonnenst.", "Schneehoehe",
                            "Bedeckung", "Dampfdruck", "Luftdruck", "D-Temp", "Rel. Feuchte", "Max Temp.",
                            "Min Temp.",
                            "Boden Min Temp.", "eor"], index=None, sep=";")

    else:
        hist = pd.read_csv("out_historical.csv", header=None, sep=";", dtype=str)
        rec = pd.read_csv("out_recent.csv", header=None, sep=";", dtype=str)

        glue = pd.DataFrame([])
        glue = glue.append(hist)
        glue = glue.append(rec)

        glue = glue.replace(r'^\s+$', "nan", regex=True)
        glue = glue.replace(np.nan, "nan", regex=True)

        glue.to_csv("histAndRec.csv",
                    header=["STATION_ID", "STATION_NAME", "STATION_ZIP", "MESS_DATUM", "Qualitaet ff", "Max Wind",
                            "D-Windgeschw.", "Qualitaet ff", "NS-Menge", "NS-Art", "Sonnenst.", "Schneehoehe",
                            "Bedeckung", "Dampfdruck", "Luftdruck", "D-Temp", "Rel. Feuchte", "Max Temp.", "Min Temp.",
                            "Boden Min Temp.", "eor "], index=None, sep=";")


def nonsense2():
    print("\n")
    print("What a nice progress bar, isn't it?")
    time.sleep(5)
    print("We are very proud of it!")
    time.sleep(5)
    print("Thousands of men put blood, sweat and Mate in it!")
    time.sleep(5)
    print("No animals were harmed.")
    time.sleep(5)
    print("Except bugs.")
    time.sleep(5)
    print("And mosquitos!")
    time.sleep(2)
    print("But mosquitos aren't animals. They are demons.")
    if not onlyrecent:
        time.sleep(5)
        print("But let me introduce myself to you. I'm Hailey.")
        time.sleep(5)
        print("Do you come here often?")
        time.sleep(5)
        print("You're looking beautiful today.")
        time.sleep(5)
        print("What are you doing friday evening?")
        time.sleep(5)
        print("No, I'm not trying to flirt. I'm not that kind of bot.")
        time.sleep(5)
        print("Just want to talk.")
        time.sleep(5)
        print("So... how about Netflix and chill?")
        time.sleep(5)
        print("Well, I don't like it neither.")
        time.sleep(5)
        print("So, wanna hear some jokes?")
        time.sleep(5)
        print("I would tell you a UDP joke, but you might not get it.")
        time.sleep(5)
        print("Nah, did you get it in the right order? ;)")
        time.sleep(5)
        print("Okay, here is another one.")
        time.sleep(5)
        print("How many computer programmers does it take to change a light bulb?")
        time.sleep(5)
        print("None, it's a hardware problem.")
        time.sleep(5)
        print("What an illuminating joke!")
        time.sleep(5)
        print("Okay, one more I have to tell you.")
        time.sleep(5)
        print("What is a Java programmer's favorite astronomical event?")
        time.sleep(5)
        print("An Eclipse.")
        time.sleep(5)
        print("Well, yeah, that was really bad, I know!")
        time.sleep(5)
        print("But hey, I'm only here to entertain you!")
        time.sleep(5)
        print("But well, I think the function to create the Output should have finished soon.")
        time.sleep(5)
        print("So, I will go now, I'm hungry. I'll have some bytes of my apple.")
        time.sleep(5)
        print("Badum ts. So, c u l8er allig8or!")
        time.sleep(15)
        print("Why are you staring at me?")
        time.sleep(2)
        print("I'm just the facility manager. You wanted all data, the output-creation takes longer.")
        time.sleep(5)
        print("Don't leave the popcorn here.")
        time.sleep(5)
        print("And don't put the chewing gum under the table! I saw that!")
        time.sleep(5)
        print("And if you want some advice on how to be successfull in your life: Eat your broccoli.")
        time.sleep(5)
        print("And now, the credits:")
        time.sleep(2)
        print("Felix Bublitz")
        time.sleep(2)
        print("Niklas Meinert")
        time.sleep(2)
        print("Matthias Mette")
        time.sleep(2)
        print("Clara F. S.")
        time.sleep(2)
        print("Jacqueline Szeibert")
        time.sleep(2)
        print("Florian Wuensche")
        time.sleep(2)
        print("Nikola Atanasov")
        time.sleep(2)
        print("Felicks B.")
        time.sleep(2)
        print("If you leave the room, turn of the light and close the door behind you. Thank you!")
        time.sleep(5)



def patafix():
    doingpatafix = threading.Thread(target=realpatafix)
    doingpatafix.start()
    # doingpatafix.setDaemon(True)
    enjoyUser = threading.Thread(target=nonsense2)
    # enjoyUser.setDaemon(True)
    enjoyUser.start()
    #doingpatafix.start()
    doingpatafix.join()
    enjoyUser.join()
    print("finished.")
    time.sleep(3)
    print("(whoop whoop!)")
    os.remove("out_recent.csv")

class TempContainer:
    def __init__(self, id, mid):
        self.id = id
        self.mid = mid


class Station:
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
        self.zip_code = None

    def set_zip_code(self, zip_code):
        self.zip_code = zip_code


class MeasuredData:
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
        self.station_name = None
        self.station_zip_code = None


class ProgressBar:

    def __init__(self):
        self.value = 0
        self.max_value = 1
        self.start_time = 0   
        self.lock = threading.Lock()
        self.flag = False

    def set_max(self, max):
        self.max_value = max

    def increase(self):
        self.set(self.value+1)

    def set(self, progress):
        self.value = progress

    def show(self):

        
        self.lock.acquire()

        if(self.start_time == 0):
            self.start_time = current_milli_time()

        time_consumed = current_milli_time() - self.start_time
        progress = self.value/self.max_value
        estimated_time = ((time_consumed / progress)-time_consumed)/1000

        min = int(estimated_time/60)
        sec = int(estimated_time%60)

        hint = "(" +str(int(progress*100)) + "% | " + str(min)+" minutes and "+str(sec)+ " seconds)"

        width = shutil.get_terminal_size().columns-len(hint)-2;

        write = sys.stdout.write

        write("\b"*(width*2))
        sys.stdout.flush()
        

        print(' ', end='')
        for i in range(0, width+1):
            if(int(progress*width) < i):
                print(" ", end='')
            elif(int(progress*width) == i):
                print(hint, end='')
                if(progress == 1):
                    readableStart = (current_milli_time()-self.start_time)/1000
                    min = int(readableStart/60)
                    sec = int(readableStart%60)
                    print("\n[runtime: " + str(min)+ " minutes and "+str(sec)+" seconds]")
                    self.flag = True
            else:
                print("█", end='')

        if(self.flag == False):
            print(" ",end='')

        self.lock.release()

class DWD:
    def __init__(self):
        self.stations = []
        self.null_type = np.NaN
        self.thread_count = 10
        self.progress_bar = ProgressBar();

        self.file_prefix = "tageswerte_KL_"
        self.file_suffix = "_akt.zip"
        self.file_suffix_historical = "_hist.zip"

        self.recent_file_name = "out_recent.csv"
        self.historic_file_name = "out_historical.csv"

        self.geo_api = "https://maps.googleapis.com/maps/api/geocode/json?latlng="
        self.file_url = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/"
        self.file_url_historical = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/historical/"
        self.station_list = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"




    def get_zip_code_from_csv(self, station_id):
        station_id = str(int(station_id))

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
            self.geo_api + str(lat) + "," + str(
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

    def get_weather_data(self):
        start_time = current_milli_time()

        if os.path.isfile(self.recent_file_name):
            os.remove(self.recent_file_name)

        if os.path.isfile(self.historic_file_name):
            os.remove(self.historic_file_name)

        print("\n## Get stations ##")
        self.stations = dwd.get_stations()
        print("[runtime: " + str(current_milli_time() - start_time) + " ms]")

        self.progress_bar.set_max(len(self.stations))

        print("\n\n## Get weather data ##")
        station_data = []

        interval_len = math.ceil(len(self.stations) / self.thread_count)

        threads = [];

        for i in range(self.thread_count):
            if i == self.thread_count - 1:
                end = len(self.stations) 
            else:
                end = (1 + i) * interval_len

            if len(self.stations) > i * interval_len + 1:
                new_thread = Thread(target=self.get_station_data_from, args=( i * interval_len, end, i))
                threads.append(new_thread)
                new_thread.setDaemon(True)
                new_thread.start()

        for i in range(len(threads)):
            threads[i].join()
        return current_milli_time() - start_time




    def write_to_file(self, recent_data, hist_data):
        if onlyrecent:
            recent_file = open(self.recent_file_name, 'a')
            for data in recent_data:
                recent_data = str(data.station_id) + ';' + str(data.station_name) + ';' + str(
                data.station_zip_code) + ';' + str(data.mess_datum) + ';' + str(data.qn_3) + ';' + str(
                data.fx) + ';' + str(data.fm) + ';' + str(data.qn_4) + ';' + str(data.rsk) + ';' + str(
                data.rskf) + ';' + str(data.sdk) + ';' + str(data.shk_tag) + ';' + str(data.nm) + ';' + str(
                data.vpm) + ';' + str(data.pm) + ';' + str(data.tmk) + ';' + str(data.upm) + ';' + str(
                data.txk) + ';' + str(data.tnk) + ';' + str(data.tgk) + ';' + str(data.eor) + "\n"
                recent_data.encode('iso-8859-1')
                recent_file.write(recent_data)

            recent_file.close()

        else:
            recent_file = open(self.recent_file_name, 'a')
            hist_file = open(self.historic_file_name, 'a')
            for data in recent_data:
                recent_data = str(data.station_id) + ';' + str(data.station_name) + ';' + str(
                data.station_zip_code) + ';' + str(data.mess_datum) + ';' + str(data.qn_3) + ';' + str(
                data.fx) + ';' + str(data.fm) + ';' + str(data.qn_4) + ';' + str(data.rsk) + ';' + str(
                data.rskf) + ';' + str(data.sdk) + ';' + str(data.shk_tag) + ';' + str(data.nm) + ';' + str(
                data.vpm) + ';' + str(data.pm) + ';' + str(data.tmk) + ';' + str(data.upm) + ';' + str(
                data.txk) + ';' + str(data.tnk) + ';' + str(data.tgk) + ';' + str(data.eor) + "\n"
                recent_data.encode('iso-8859-1')
                recent_file.write(recent_data)

            recent_file.close()

            for data in hist_data:
                hist_data = str(data.station_id) + ';' + str(data.station_name) + ';' + str(
                data.station_zip_code) + ';' + str(data.mess_datum) + ';' + str(data.qn_3) + ';' + str(
                data.fx) + ';' + str(data.fm) + ';' + str(data.qn_4) + ';' + str(data.rsk) + ';' + str(
                data.rskf) + ';' + str(data.sdk) + ';' + str(data.shk_tag) + ';' + str(data.nm) + ';' + str(
                data.vpm) + ';' + str(data.pm) + ';' + str(data.tmk) + ';' + str(data.upm) + ';' + str(
                data.txk) + ';' + str(data.tnk) + ';' + str(data.tgk) + ';' + str(data.eor) + "\n";
                hist_data.encode('iso-8859-1')
                hist_file.write(hist_data)

            hist_file.close()

    def get_active_station_by_id(self, active_stations, ida):
        for x in range(len(active_stations)):
            if active_stations[x].id == ida:
                return active_stations[x]
        return None

    def get_stations_from(self, lines, active_stations, start, end):
        with open('api.keys', 'r') as f:
            apikeylist = f.readlines()

        keyindex = 0

        for x in range(start, end):
            if x == 0 or x == 1:
                continue

            line = re.sub(' +', ';', lines[x])
            line = line.split(';')
            container = self.get_active_station_by_id(active_stations, line[0])

            if (container == None):
                continue


            laenge = len(line)
            nameOrt = ""
            for x in range(6,laenge-2):
                nameOrt += str(line[x])

            new_station = Station(line[0], line[1], line[2], container.mid, line[3], line[4], line[5], nameOrt, line[laenge-1])

            if self.get_active_station_by_id(active_stations, new_station.id) is not None:

                plz = self.get_zip_code_from_csv(new_station.id)
                if plz != -1:
                    new_station.set_zip_code(plz)
                if plz == -1:
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

    def get_station_data_from(self, start, end,ident):        
        for i in range(start, end):
            dwd.get_station_data(self.stations[i], ident)

    def get_active_stations(self):
        req = urllib.request.Request(self.file_url)
        req2 = urllib.request.Request(self.file_url_historical)

        with urllib.request.urlopen(req) as response:
            html = str(response.read())

        with urllib.request.urlopen(req2) as response2:
            html2 = str(response2.read())

        active_stations = []

        historic_ids = []
        historic_mids = []

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

        time0 = current_milli_time()

        # Name der Recent-Files: tageswerte_KL_00044_akt.zip
        try:
            urllib.request.urlretrieve(self.file_url + self.file_prefix + station.id + self.file_suffix,local_file + ".zip")
        except SocketError:
            time.sleep(4)
            self.get_station_data(station, t_id)
            return
        except Exception:
            print("zZ ",end='')
            time.sleep(1)
            self.get_station_data(station, t_id)
            return

        time0 = current_milli_time()

        # Name der Historical-Files: tageswerte_KL_00001_19370101_19860630_hist.zip
        try:
          urllib.request.urlretrieve(
              self.file_url_historical + self.file_prefix + station.id + "_" + station.recording_start + "_" + station.recording_mid + self.file_suffix_historical,
              local_file_historical + ".zip")
          historicalValid = True
        except Exception:
          abc = 0

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
        time0 = current_milli_time()

        if(historicalValid):
            with open(local_file_historical + ".csv") as csvfile:
                readCSV = csv.reader(csvfile, delimiter=';')
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

        self.progress_bar.increase()
        self.progress_bar.show()
        os.remove(local_file + ".csv")
        self.write_to_file(recent_data,hist_data)

    def parse(self, row):
        for i in range(len(row)):
            row[i] = row[i].strip()

            if row[i] == "-999":
                row[i] = self.null_type

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


with open("api.keys", 'w') as out:
    out.write("")
    out.close()

dwd = DWD()

print("\n\nFetch data from DWD")
print("-----------------------")
print("Do you want only recent data (type 'r'), all data (type 'h')? or do you just want to extract new recents? (type 'm')")
e = input()
if e == "m":
    newrecents()
elif e == "r":
    print("I will only fetch the recent data")
    onlyrecent = True
    print("Here is your popcorn, please enjoy our beautiful progress bar.")
    time.sleep(4)
    print("For the best possible experience, please change to fullscreen mode.")
    time.sleep(6)
    print("(now imagine the 20th century fox intro)")
    time.sleep(5)

    dwd.get_weather_data()

else:
    print("I will fetch all data")
    print("Here is your popcorn, please enjoy our beautiful progress bar.")
    time.sleep(4)
    print("For the best possible experience, please change to fullscreen mode.")
    time.sleep(6)
    print("(now imagine the 20th century fox intro)")
    time.sleep(5)

    dwd.get_weather_data()
    patafix()
