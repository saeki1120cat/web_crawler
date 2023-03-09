import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime
import os
import pyodbc
import urllib.request
import zipfile 

# Set today's date
today = str(datetime.date.today())

# Download Weather Data
res = "http://opendata.cwb.gov.tw/opendataapi?dataid=F-D0047-093&authorizationkey=CWB-3FB0188A-5506-41BE-B42A-3785B42C3823"
urllib.request.urlretrieve(res, "F-D0047-093.zip")
with zipfile.ZipFile("F-D0047-093.zip") as myzip:
    files = myzip.namelist()
    CITY, DISTRICT, GEOCODE, DAY, TIME, T, TD, RH, WD, WS, BF, AT, Wx, Wx_n, PoP6h, PoP12h, get_day = ([] for _ in range(17))
    for filename in files:
        try:
            with myzip.open(filename) as myfile:
                soup = BeautifulSoup(myfile.read(), "xml")
                city = soup.locationsName.text
                for loc in soup.find_all("location"):
                    district = loc.find_all("locationName")[0].text
                    geocode = loc.geocode.text
                    weather = loc.find_all("weatherElement")
                    time = [t.text.split("T") for t in weather[1].find_all("dataTime")]
                    for t in time:
                        DAY.append(t[0])
                        TIME.append(t[1].split("+")[0])
                        CITY.append(city)
                        DISTRICT.append(district)
                        GEOCODE.append(geocode)
                        get_day.append(today)
                    for i, values in enumerate([T, TD, RH, WD, AT]):
                        for v in weather[i].find_all("value"):
                            values.append(v.text)
                    ws = weather[6].find_all("value")
                    for k in range(0, len(ws), 2):
                        WS.append(ws[k].text)
                        BF.append(ws[k+1].text)
                    wx = weather[9].find_all("value")
                    for w in range(0, len(wx), 2):
                        Wx.append(wx[w].text)
                        Wx_n.append(wx[w+1].text)
                    for r in weather[3:5]:
                        for v in r.find_all("value"):
                            PoP6h.append(v.text)
                            PoP12h.append(v.text)
                            if r == weather[4]:
                                PoP12h.append(v.text)
                myfile.close()
        except:
            break

# Safe the data as a DataFrame
data = {
    "CITY": CITY, "DISTRICT": DISTRICT, "GEOCODE": GEOCODE, "DAY": DAY, "TIME": TIME, "T": T, "TD": TD, "RH": RH,
    "WD": WD, "WS": WS, "BF": BF, "AT": AT, "Wx": Wx, "Wx_n": Wx_n, "PoP6h": PoP6h, "PoP12h": PoP12h, "get_day": get_day
}
df = pd.DataFrame(data)

# convert date format
df["DAYTIME"] = pd.to_datetime(df["DAY"] + " " + df["TIME"], format="%Y-%m-%d %H:%M:%S")

day_numbers = [int(day.split("-")[2]) for day in days]

# Create a DataFrame from the day numbers
d_n = pd.DataFrame(day_numbers, columns=["d_n"])

# Combine d_n DataFrame with df DataFrame
df = pd.concat([df, d_n], 1)

# Extract file day number from today's date
file_day = int(today.split("-")[2]) + 1

# Filter df DataFrame by file day number and exclude presidential palace
df = df[(df.d_n == file_day) & (df.DISTRICT != "presidential palace")]

# Select desired columns and save as a CSV file
df[["CITY", "DISTRICT", "GEOCODE", "DAYTIME", "T", "TD", "RH", "WD", "WS", "BF", "AT", "Wx", "Wx_n", "PoP6h", "PoP12h", "get_day"]].to_csv(save_name, index=False, encoding="utf_8_sig")

# Convert df DataFrame to a list of lists
listdata = df.values.tolist()

# Connect to the database using pyodbc
server = '168.14.xx.xx'
username = 'username'
password = 'password'
database = 'database name'
driver = '{ODBC Driver 13 for SQL Server}'
connectionString = 'DRIVER={0};PORT=1433;SERVER={1};DATABASE={2};UID={3};PWD={4}'.format(driver, server, database, username, password)
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()

# Iterate through listdata and insert each record into the database
for d in listdata:
    insertSql = "insert into [dbname].[dbo].[WEATHER_CRAWLER]([CITY],[DISTRICT],[GEOCODE],[DAYTIME],[T],[TD],[RH],[WD],[WS],[BF],[AT],[Wx],[Wx_n],[PoP6h],[PoP12h],[get_day])values (?, ?, ?, convert(datetime, ?, 120), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, convert(date, ?, 120))"
    cursor.execute(insertSql, d)

# Commit and close the cursor and connection
cursor.commit()
cursor.close()
cnxn.close()