import pandas as pd
from influxdb import DataFrameClient
import numpy as np

def convertTime(s):
    try:
        dt = pd.datetime.strptime(s, '%d.%m.%Y %H:%M:%S.%f')
        return dt
    except:
        return None

df = pd.read_csv('WORK.csv', delimiter=';', skiprows=[0,2,3,4])
print(df.head())
df["time"] = df["time"].map(convertTime)
df = df.set_index("time")
print(df.head())

client = DataFrameClient(host='localhost', port=8086 )
client.switch_database('HOUSE')
#client.write_points(df, 'iba')
nRows, nCols = df.shape
K = 1000
n0 = 0
while n0 < nRows:
    n1 = min(n0 + K, nRows)
    print(n0, n1)
    client.write_points(df[n0:n1], 'WORK', tag_columns=['COIL_ID'])
    n0 = n1