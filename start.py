import os

print("What do you want me to do?\n1) Create one big file from historical and recent data\n2) Only attach the latest recent data to the final.csv\n3) Get recent or historical data in a dedicated file")

while True:
    print("Please type in the related number:")
    input_var = input("")
    if input_var == "1" or input_var == "2" or input_var == "3":
        break

if input_var == "1":
   input_var = "finalmain.py"
elif input_var == "2":
    input_var = "recentData.py"
elif input_var == "3":
    input_var = "recentOrHist.py"


os.system('python3 '+str(input_var))
