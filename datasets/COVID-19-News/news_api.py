import numpy
import math
import pandas as pd
import csv 
import requests
import json
import os

from newsapi import NewsApiClient

newsapi = NewsApiClient(api_key='################################')

gnews_articles = newsapi.get_everything(q='covid',
                                      sources='google-news',
                                      from_param='2020-10-29',
                                      to='2020-11-28',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

cnn_articles = newsapi.get_everything(q='covid',
                                      sources='cnn',
                                      from_param='2020-10-29',
                                      to='2020-11-28',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

fox_articles = newsapi.get_everything(q='covid',
                                      sources='fox-news',
                                      from_param='2020-10-29',
                                      to='2020-11-28',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

wsj_articles = newsapi.get_everything(q='covid',
                                      sources='the-wall-street-journal',
                                      from_param='2020-10-29',
                                      to='2020-11-28',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

usa_today_articles = newsapi.get_everything(q='covid',
                                      sources='usa-today',
                                      from_param='2020-10-29',
                                      to='2020-11-28',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)


gnews_json = json.dumps(gnews_articles)
gnews_json = json.loads(gnews_json)
gnews_articles = gnews_json["articles"]

cnn_json = json.dumps(cnn_articles)
cnn_json = json.loads(cnn_json)
cnn_articles = cnn_json["articles"]

fox_json = json.dumps(fox_articles)
fox_json = json.loads(fox_json)
fox_articles = fox_json["articles"]

wsj_json = json.dumps(wsj_articles)
wsj_json = json.loads(wsj_json)
wsj_articles = wsj_json["articles"]

usa_today_json = json.dumps(usa_today_articles)
usa_today_json = json.loads(usa_today_json)
usa_today_articles = usa_today_json["articles"]

file_list_names = ["usa_today_articles.csv","wsj_articles.csv","fox_articles.csv","gnews_articles.csv","cnn_articles.csv"]
api_objects = [usa_today_articles,wsj_articles,fox_articles,gnews_articles,cnn_articles]

def jsontocsv(json_object,filename):
    with open(filename,"w",newline="") as f: 
        title = "source,author,title,description,url,urlToImage,publishedAt,content".split(",")
        cw = csv.DictWriter(f,title,delimiter=',')
        cw.writeheader()
        cw.writerows(json_object)


for i in range(len(api_objects)):
    jsontocsv(api_objects[i],file_list_names[i])

gnews_articles = pd.read_csv("./gnews_articles.csv")
gnews_articles['source'] = gnews_articles['source'].apply(lambda x: 'news.google.com')

cnn_articles = pd.read_csv("./cnn_articles.csv")
cnn_articles['source'] = cnn_articles['source'].apply(lambda x: 'cnn.com')

fox_articles = pd.read_csv("./fox_articles.csv")
fox_articles['source'] = fox_articles['source'].apply(lambda x: 'foxnews.com')

wsj_articles = pd.read_csv("./wsj_articles.csv")
wsj_articles['source'] = wsj_articles['source'].apply(lambda x: 'wsj.com')

usa_today_articles = pd.read_csv("./usa_today_articles.csv")
usa_today_articles['source'] = usa_today_articles['source'].apply(lambda x: 'usatoday.com')

newsapi_result = [gnews_articles,cnn_articles,fox_articles,wsj_articles,usa_today_articles]
newsapi_articles = pd.concat(newsapi_result)

newsapi_articles = newsapi_result.assign(date = lambda x: x['publishedAt'])
newsapi_articles = newsapi_articles.drop(['author', 'urlToImage','publishedAt','content'], axis=1)
newsapi_articles['date'] = newsapi_articles['date'].apply(lambda x: str(x[0:10]))

newsapi_articles = newsapi_articles.rename(columns={'description': 'summary'})
newsapi_articles = newsapi_articles[['date','title','summary','source','url']]

newsapi_articles.to_csv("news_api.csv", encoding='utf-8', index=False)
