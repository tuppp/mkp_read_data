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
  station_id = None
  station_name = None
  station_plz = None

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

  set_station_data(id,name,plz):
    station_id = id
    station_name = name
    station_plz = plz

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

  #saves a list of cleaned weather data as csv
  get_weather_data:
    stations = dwd.get_stations()

    station_data = []

    for station in stations:
      station_data.append(dwd.get_station_data("00078", start_date))


    full_list = concate_lists(station_data)

    export_csv(full_list)

    return true
  #

  get_stations:
  #...
  #...


  get_station_data:
    #...
    #get_zip_code(lat,lon)
    #

  get_zip_code(lat, lon):
    zip_code = 0
    return zip_code



dwd = DWD()

dwd.get_weather_data();