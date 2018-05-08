
import csv
import numpy as np
import pandas as pd


reader = csv.reader(open("testdata.csv", "r"), delimiter=";")
x = list(reader)
result = np.array(x).astype(str)

result = np.delete(result, 2, 1)
result = np.delete(result, 2, 1)
result = np.delete(result, 3, 1)
result = np.delete(result, 5, 1)
result = np.delete(result, 5, 1)
result = np.delete(result, 5, 1)
result = np.delete(result, 5, 1)
result = np.delete(result, 5, 1)
result = np.delete(result, 10, 1)
result = np.delete(result, 9, 1)
result = np.delete(result, 8, 1)
result = np.delete(result, 7, 1)
result = np.delete(result, 6, 1)

for i in range(result.shape[1]):
    if result[0][i] == "  FM":
        result[0][i] = "D-Windgeschw."
    elif result[0][i] == " RSK":
        result[0][i] = "NS-Menge"
    elif result[0][i] == "RSKF":
        result[0][i] = "NS-Art"
    elif result[0][i] == " TMK":
        result[0][i] = "D-Temp"

for i in range(result.shape[0]):
    if result[i][2] == "-999":
        result[i][2] = np.NAN

for j in range(result.shape[0]):
    if result[j][4] == "   0":
        result[j][4] = "kein NS"
    elif result[j][4] == "1":
        result[j][4] = "Regen"
    elif result[j][4] == "   4":
        result[j][4] = "Form unbekannt"
    elif result[j][4] == "   6":
        result[j][4] = "Regen"
    elif result[j][4] == "   7":
        result[j][4] = "Schnee"
    elif result[j][4] == "   8":
            result[j][4] = "Schneeregen"
    elif result[j][4] == "   9":
        result[j][4] = "Nicht feststellbar"

df = pd.DataFrame(result)
df.to_csv("result.csv", header=None, index=None)
