import numpy
import math
import pandas as pd
import csv 
import requests
import json
import os

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","to":"2020/05/31","lang":"en","media":"True","from":"2020/05/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

may_response = requests.request("GET", url, headers=headers, params=querystring)

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","to":"2020/06/30","lang":"en","media":"True","from":"2020/06/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

june_response = requests.request("GET", url, headers=headers, params=querystring)

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","to":"2020/07/31","lang":"en","media":"True","from":"2020/07/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

july_response = requests.request("GET", url, headers=headers, params=querystring)

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","to":"2020/08/31","lang":"en","media":"True","from":"2020/08/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

august_response = requests.request("GET", url, headers=headers, params=querystring)

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","to":"2020/09/30","lang":"en","media":"True","from":"2020/09/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

september_response = requests.request("GET", url, headers=headers, params=querystring)

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","to":"2020/10/31","lang":"en","media":"True","from":"2020/10/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

october_response = requests.request("GET", url, headers=headers, params=querystring)

url = "https://covid-19-news.p.rapidapi.com/v1/covid"

querystring = {"q":"covid","lang":"en","media":"True","from":"2020/11/01"}

headers = {
    'x-rapidapi-key': "84010bc0damsh9e9ba15f04e16e3p112d40jsn09225ea01f32",
    'x-rapidapi-host': "covid-19-news.p.rapidapi.com"
    }

november_response = requests.request("GET", url, headers=headers, params=querystring)

may_json = json.loads(may_response.text)
june_json = json.loads(june_response.text)
july_json = json.loads(july_response.text)
august_json = json.loads(august_response.text)
september_json = json.loads(september_response.text)
october_json = json.loads(october_response.text)
november_json = json.loads(november_response.text)

may_articles = may_json["articles"]

# Depending on what form the JSON comes, you may need to manually remove some instances
# del may_articles[11]

june_articles = june_json["articles"]
july_articles = july_json["articles"]
august_articles = august_json["articles"]
september_articles = september_json["articles"]
october_articles = october_json["articles"]
november_articles = november_json["articles"]

file_list_names = ["may_articles.csv","june_articles.csv","july_articles.csv","august_articles.csv","september_articles.csv","october_articles.csv","november_articles.csv"]
api_objects = [may_articles,june_articles,july_articles,august_articles,september_articles,october_articles,november_articles]

def jsontocsv(json_object,filename):
    with open(filename,"w",newline="") as f: 
        title = "summary,country,clean_url,author,rights,link,rank,topic,language,title,published_date,_id,_score".split(",")
        cw = csv.DictWriter(f,title,delimiter=',')
        cw.writeheader()
        cw.writerows(json_object)
        
for i in range(len(api_objects)):
    jsontocsv(api_objects[i],file_list_names[i])

all_covid_articles = [may_articles,june_articles,july_articles,august_articles,september_articles,october_articles,november_articles]

covid_19_news_api_articles = pd.concat(all_covid_articles)

covid_19_news_api_articles = covid_19_news_api_articles.assign(date = lambda x: x['published_date'])
covid_19_news_api_articles = covid_19_news_api_articles.drop(['country', 'author','rights','rank','topic','language','_id','_score','published_date', 'media','media','media_content'], axis=1)
covid_19_news_api_articles['date'] = covid_19_news_api_articles['date'].apply(lambda x: str(x[0:10]))

covid_19_news_api_articles = covid_19_news_api_articles.rename(columns={'clean_url': 'source','link':'url'})
covid_19_news_api_articles = covid_19_news_api_articles[['date','title','source']]
covid_19_news_api_articles = covid_19_news_api_articles.sort_values(by=['date'])

covid_19_news_api_articles.to_csv('covid_19_news_api_articles.csv')
