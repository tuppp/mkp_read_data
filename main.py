class DWD:


  class Station:
    #...
    #...

  class MeasuredData:
    #...
    #...


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