import pandas as pd
import numpy as np
from numpy import inf


base_url = "csse_covid_19_time_series/"
confirmed_url = "time_series_covid19_confirmed_global.csv"
dead_url = "time_series_covid19_deaths_global.csv"

'''
Aggregating US Covid cases
'''
#Confirmed cases
df = pd.read_csv(base_url + confirmed_url)
confirmed_copy = df.copy()
df = df.drop(["Lat", "Long"], axis=1) #drop latitude and longitude
confirmed = df.drop("Province/State", axis=1)  # take only countries

#Covid deaths
df = pd.read_csv(base_url + dead_url)
dead_copy = df.copy()
df = df.drop(["Lat", "Long"], axis=1) #drop latitude and longitude
dead = df.drop("Province/State", axis=1) # take only countries (no territories)

dates = np.intersect1d(confirmed.drop("Country/Region", axis=1).columns, dead.columns)

countries = ["US"]

data = pd.DataFrame()

for country in countries:
    # confirmed cases
    cntry_c = (
        confirmed[confirmed["Country/Region"] == country]
        .transpose()
        .drop("Country/Region")
    )
    cntry_c.columns = ["Confirmed Cases"]
    cntry_c["Date"] = cntry_c.index

    # total deaths
    cntry_d = (
        dead[dead["Country/Region"] == country]
        .transpose()
        .drop("Country/Region")
    )
    cntry_d.columns = ["Deaths"]
    cntry_d["Date"] = cntry_d.index
    
    # concatenate
    cntry = cntry_c
    cntry["Deaths"] = cntry_d["Deaths"]

    cntry = cntry.reset_index(drop=True)
    cntry["Country"] = country
    # print(cntry)
    data = data.append(
        cntry[["Date", "Country", "Confirmed Cases","Deaths"]]
    )

def adjust_date(s):
    l = s.split("/")
    return f"20{l[2]}-{int(l[0]):02d}-{int(l[1]):02d}"

data["Date"] = data["Date"].map(adjust_date)
data = data.reset_index(drop=True)

data['Cases Increase'] = data['Confirmed Cases'].pct_change(1).fillna(0)
data['Deaths Increase'] = data['Deaths'].pct_change(1).fillna(0)
data['Deaths Increase'] = data['Deaths Increase'].replace(inf,0)

data.to_csv("us-covid19-aggregate.csv", index=False)

'''
Aggregating World Covid cases
'''
confirmed = confirmed_copy.copy().drop(
    ["Lat", "Long", "Province/State", "Country/Region"], axis=1
)
dead = dead_copy.copy().drop(
    ["Lat", "Long", "Province/State", "Country/Region"], axis=1
)

df = pd.DataFrame()
df["Confirmed Cases"] = confirmed.sum()
df["Deaths"] = dead.sum()
df["Date"] = df.index
df["Date"] = df["Date"].map(adjust_date)
df = df.reset_index(drop=True)
df = df[["Date", "Confirmed Cases", "Deaths"]]

df['Cases Increase'] = df['Confirmed Cases'].pct_change(1).fillna(0)
df['Deaths Increase'] = df['Deaths'].pct_change(1).fillna(0)
df['Deaths Increase'] = df['Deaths Increase'].replace(inf,0)

df.to_csv("worldwide-covid19-aggregate.csv", index=False)