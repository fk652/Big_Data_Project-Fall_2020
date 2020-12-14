import numpy
import math
import pandas as pd
import csv 
import requests
import json
import os
import datetime


from newsapi import NewsApiClient

# Enter your own News API Key! 
newsapi = NewsApiClient(api_key='################################')

# Get Google News Articles from NewsAPI
gnews_articles = newsapi.get_everything(q='covid',
                                      sources='google-news',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

# Get CNN articles from NewsAPI
cnn_articles = newsapi.get_everything(q='covid',
                                      sources='cnn',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

# Get FOX articles from NewsAPI
fox_articles = newsapi.get_everything(q='covid',
                                      sources='fox-news',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

# Get WSJ articles from NewsAPI
wsj_articles = newsapi.get_everything(q='covid',
                                      sources='the-wall-street-journal',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

# Get USA Today articles from News API
usa_today_articles = newsapi.get_everything(q='covid',
                                      sources='usa-today',
                                      language='en',
                                      sort_by='relevancy',
                                      page=1)

# Modify JSON format into Pandas DataFrame
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

# Make Parsed JSON objects into CSV
def jsontocsv(json_object,filename):
    with open(filename,"w",newline="") as f: 
        title = "source,author,title,description,url,urlToImage,publishedAt,content".split(",")
        cw = csv.DictWriter(f,title,delimiter=',')
        cw.writeheader()
        cw.writerows(json_object)


for i in range(len(api_objects)):
    jsontocsv(api_objects[i],file_list_names[i])

# Read API news from local stored files
# Modify source feature 

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

# Concatenate news articles
newsapi_result = [gnews_articles,cnn_articles,fox_articles,wsj_articles,usa_today_articles]
newsapi_articles = pd.concat(newsapi_result)

# Create date column
newsapi_articles = newsapi_result.assign(date = lambda x: x['publishedAt'])

# Drop columns
newsapi_articles = newsapi_articles.drop(['author', 'urlToImage','publishedAt','content'], axis=1)

# Format news
newsapi_articles['date'] = newsapi_articles['date'].apply(lambda x: str(x[0:10]))

# Rename column
newsapi_articles = newsapi_articles.rename(columns={'description': 'summary'})

# Reorder columns
newsapi_articles = newsapi_articles[['date','title','summary','source','url']]

# Save as csv file news_api.csv
newsapi_articles.to_csv("news_api.csv", encoding='utf-8', index=False)

file_list_names = ["./usa_today_articles.csv","./wsj_articles.csv","./fox_articles.csv","./gnews_articles.csv","./cnn_articles.csv"]

# Cleanup files you created
for file_path in file_list_names:
    os.remove(file_path)
