import shutil
import csv
import zipfile
import mimetypes
import os
#import urllib2
import urllib.parse
import urllib.request
import csv
import pandas as pd
import numpy as np
import chardet

def filename_from_url(url):
    return os.path.basename(urllib.parse.urlsplit(url)[2])


def download_file(url):
    """Create an urllib2 request and return the request plus some useful info"""
    name = filename_from_url(url)
    r = urllib.request.urlopen(urllib.request.Request(url))
    info = r.info()
    if 'Content-Disposition' in info:
        # If the response has Content-Disposition, we take filename from it
        name = info['Content-Disposition'].split('filename=')[1]
        if name[0] == '"' or name[0] == "'":
            name = name[1:-1]
    elif r.geturl() != url:
        # if we were redirected, take the filename from the final url
        name = filename_from_url(r.geturl())
    content_type = None
    if 'Content-Type' in info:
        content_type = info['Content-Type'].split(';')[0]
    # Try to guess missing info
    if not name and not content_type:
        name = 'unknown'
    elif not name:
        name = 'unknown' + mimetypes.guess_extension(content_type) or ''
    elif not content_type:
        content_type = mimetypes.guess_type(name)[0]
    return r, name, content_type


def download_file_locally(url, dest):
    req, filename, content_type = download_file(url)
    if dest.endswith('/'):
        dest = os.path.join(dest, filename)
    with open(dest, 'wb') as f:
        shutil.copyfileobj(req, f)


def getZips(stationlist):
    first = False
    if os.path.isfile("result.csv"):
        os.remove("result.csv")
    for station in stationlist:
        print(station)
        try:


            tmp = urllib.request.urlopen("ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/tageswerte_KL_" + station + "_akt.zip")

            zipcontent = tmp.read()

            with open(station + ".zip", 'wb') as f:
                f.write(zipcontent)
                name = station + ".zip"
            zf = zipfile.ZipFile(name, "r")

            target = zf.open(str(zf.namelist()[-1]))

            print(type(target))

            csv = open("result.csv", "at")
            targetbytes = target.read()
            enc = chardet.detect(targetbytes).get("encoding")

            targetstring = str(targetbytes, encoding=enc)
            #print(type(targetstring))
            targetstring = targetstring.replace("-999", "None")
            print(len(targetstring.splitlines()))
            if first:
                targetstring = targetstring.replace("STATIONS_ID;MESS_DATUM;QN_3;  FX;  FM;QN_4; RSK;RSKF; SDK;SHK_TAG;  NM; VPM;  PM; TMK; UPM; TXK; TNK; TGK;eor", "")
            csv.write(targetstring)

            csv.close()
            first = True
            os.remove(station + ".zip")
        except (urllib.error.URLError, ValueError) as e:
            continue


def csvparser():
    result = np.genfromtxt("result.csv", delimiter=";", dtype=str)
    print(result.shape)
    for i in range(result.shape[1]):
        if "FM" in result[0][i]:
            result[0][i] = "D-Windgeschw."
        elif "RSKF" in result[0][i]:
            result[0][i] = "NS-Art"
        elif "RSK" in result[0][i]:
            result[0][i] = "NS-Menge"
        elif "TMK" in result[0][i]:
            result[0][i] = "D-Temp"
        elif "QN_3" in result[0][i]:
            result[0][i] = "Qualitaet"
        elif "FX" in result[0][i]:
            result[0][i] = "Max. Wind"
        elif "QN_4" in result[0][i]:
            result[0][i] = "Qualitaet"
        elif "SDK" in result[0][i]:
            result[0][i] = "Sonnendauer"
        elif "SHK_TAG" in result[0][i]:
            result[0][i] = "Schneehoehe"
        elif "NM" in result[0][i]:
            result[0][i] = "Bedeckung"
        elif "VPM" in result[0][i]:
            result[0][i] = "Dampfdruck"
        elif "UPM" in result[0][i]:
            result[0][i] = "Rel. Feuchte"
        elif "PM" in result[0][i]:
            result[0][i] = "Luftdruck"
        elif "TXK" in result[0][i]:
            result[0][i] = "Max. Tmp"
        elif "TGK" in result[0][i]:
            result[0][i] = "Min. Tmp Boden"
        elif "TNK" in result[0][i]:
            result[0][i] = "Min. Tmp"

    for j in range(result.shape[0]):
        if result[j][7] == "   0":
            result[j][7] = "kein NS"
        elif result[j][7] == "1":
            result[j][7] = "Regen"
        elif result[j][7] == "   4":
            result[j][7] = "Form unbekannt"
        elif result[j][7] == "   6":
            result[j][7] = "Regen"
        elif result[j][7] == "   7":
            result[j][7] = "Schnee"
        elif result[j][7] == "   8":
            result[j][7] = "Schneeregen"
        elif result[j][7] == "   9":
            result[j][7] = "Nicht feststellbar"

    df = pd.DataFrame(result)
    df.to_csv("result2.csv", header=None, index=None)


def getNamesAndZIP():
    enc =chardet.detect(open("data.csv", "rb").read()).get("encoding")
    print(enc)
    namelist = []
    f = open("data.txt", encoding=enc)
    for row in f:

        namelist.append(row.split()[6])
    namelist.pop(0)
    namelist.pop(0)
    print(namelist)

def patafix():
    hist = pd.read_csv("out_historical.csv", header=None, sep=";")
    rec = pd.read_csv("out_recent.csv", header= None, sep=";")
    glue = pd.DataFrame([])
    glue = glue.append(hist)
    glue = glue.append(rec)
    glue = glue.replace(r'^\s+$', "nan", regex=True)
    glue = glue.replace(np.nan, "nan", regex=True)
    glue.to_csv("final.csv", header=["STATION_ID", "STATION_NAME", "STATION_ZIP", "MESS_DATUM", "Qualität ff", "Max Wind", "D-Windgeschw.", "Qualität ff", "NS-Menge", "NS-Art", "Sonnenst.", "Schneehöhe", "Bedeckung", "Dampfdruck", "Luftdruck", "D-Temp", "Rel. Feuchte", "Max Temp.", "Min Temp.", "Boden Min Temp.", "eor "], index=None, sep=";")

def main():
    """download_file_locally("ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt", "data.csv")

    print("got csv of all stations, trying to parse the csv")
    enc = chardet.detect(open("data.txt", "rb").read()).get("encoding")
    stationlist = []
    with open("data.txt", encoding=enc) as f:
        reader = csv.reader(f)
        for row in reader:
            stationlist.append(''.join(row).split(" ")[0])
    stationlist.pop(0)
    stationlist.pop(0)
    print("got a list with all stationIDs")
    getZips(stationlist)
    csvparser()
    getNamesAndZIP()"""
    patafix()



if __name__ == "__main__":
    main()