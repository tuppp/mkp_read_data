import urllib.request
import zipfile
import os
import fnmatch
import shutil
import csv
import re
import json

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

class MeasuredData:
  
  
  station_name = None
  station_plz = None

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

  set_station_data(name,plz):
    station_name = name
    station_plz = plz
    
  get_station_by_id(id, stations):
    for station in stations:
      if station.id == id:
        return station
    return null

  def get_plz_from_geo(lat, lng):
    contents = urllib.request.urlopen("https://maps.googleapis.com/maps/api/geocode/json?latlng="+str(lat)+","+str(lng)+"&sensor=false")

     j = json.load(contents)
     print ("Das Json geloaded")
     print (j)

     print("\nDas Json geparst")
     plz = j['results'][0]['address_components'][7]['long_name']
     #print (plz)
     return plz


  def __init__(self, stations_id,mess_datum,qn_3,fx,fm,qn_4,rsk,rskf,sdk,shk_tag,nm,vpm,pm,tmk,upm,txk,tnk,tgk,eor):
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

  file_prefix = "tageswerte_KL_"
  file_suffix = "_akt.zip"
  file_url = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/"
  station_list = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"
  fetch_start_date = 20180508


  #saves a list of cleaned weather data as csv
  get_weather_data:
    stations = dwd.get_stations()

    station_data = []

    for station in stations:
      station_data.append(dwd.get_station_data(station.id, self.fetch_start_date))


    full_list = concatenate_lists(station_data)

    export_csv(full_list)

    return true
  #

  concatenate_lists(station_data):
    completelist = []
    for station in stationdata:
      completelist += station
    return completelist


  get_stations(self):
    urllib.request.urlretrieve(self.station_list, "temp")
      with open("temp", 'r', encoding='cp1252') as f:
        lines = f.readlines()

      stations = []

      for x in range(2,len(lines)):
        lines[x] = re.sub(' +','',lines[x])
        line = lines[x].split(';')
        new_station = Station(line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7])
        stations.append(new_station)

      os.remove("temp")

      return stations


  get_station_data(self, station_id, start_date):
    local_file = "station_" + station_id

    urllib.request.urlretrieve(self.file_url + self.file_prefix + station_id + self.file_suffix, local_file + ".zip")
    zip_ref = zipfile.ZipFile(local_file + ".zip", 'r')
    zip_ref.extractall(local_file)
    zip_ref.close()
    os.remove(local_file + ".zip")

    for file in os.listdir(local_file):
      if fnmatch.fnmatch(file, "produkt_klima_tag*"):
        os.rename(local_file + "/"+ file, local_file + ".csv")

    shutil.rmtree(local_file)

    data = []

    with open(local_file + ".csv") as csvfile:
      readCSV = csv.reader(csvfile, delimiter=';')
      first_row = True
      for row in readCSV:
          if(first_row == False):

            #!!!
            #TODO: Checken, ob Eintrag älter als start_date
            #!!!

            current_data = MeasuredData(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18])
            stations = dwd.get_stations()
            station = get_station_by_id(current_data.id, stations)
            if station != null:
              current_data.set_station_data(station.name, station.zip_code)


            data.append(current_data)
            

          else:
            first_row = False
    os.remove(local_file + ".csv")
    return data

  get_zip_code(lat, lon):
    zip_code = 0
    return zip_code



dwd = DWD()

dwd.get_weather_data();