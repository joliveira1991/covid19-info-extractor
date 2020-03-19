# Basic Imports
import requests
import time
import os
import re
import json
from attrdict import AttrDict

# import ignite
from decimal import Decimal
from pyignite import Client


# NLTK Imports
#import nltk
#from sklearn.feature_extraction.text import CountVectorizer
#from sklearn.naive_bayes import MultinomialNB

NEWS_TABLE_NAME = 'news_history'

NEWS_CREATE_TABLE_QUERY = '''CREATE TABLE if not exists news_history (
    epoch bigint,
    source varchar(200),
    title varchar(4000),
    length integer,
    date varchar(500),
    description varchar(2000),
    content varchar(4000),
    url varchar(1000),
    author varchar(1000),
    primary key (source,title)
)'''

class NewsClient(object):
    '''
    Generic News Class to check truth
    '''
    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens
        self.consumer_key = 'XXXXXXXXXXXXXXXXXXX'
        self.url = 'http://newsapi.org/v2/top-headlines'

        # attempt to connect ignite
        try:
            # establish connection
            self.client = Client()
            self.client.connect('127.0.0.1', 10800)

            # create tables
            for query in [
                NEWS_CREATE_TABLE_QUERY,
            ]:
                self.client.sql(query) 

        except Exception as ex:
            print("Error: Error on connection to ignite")
            print(ex)

    def get_news_to_ignite(self,query,category):
        '''
        Main function to fetch news and save on ignite.
        '''
        # empty list to store parsed tweets
        news = []

        try:
            # call news api to fetch tweets
            
            if category is None:
                params = dict(
                   country='pt',
                   q=query,
                   apiKey=self.consumer_key
                )
            else:
                params = dict(
                   country='pt',
                   q=query,
                   category=category,
                   apiKey=self.consumer_key
            )
            

            resp = requests.get(url=self.url, params=params)
            data = resp.content
            data_dict = json.loads(data)
            asAttribute=AttrDict(data_dict)

            for article in asAttribute.articles:
                source=article.source.name
                title=article.title
                author=article.author
                date=article.publishedAt
                content=article.content
                description=article.description
                length=len(str(content))
                url=article.url

                # if not exists, insert
                insert_statment = 'INSERT INTO news_history (epoch,source,title,length,date,description,content,url,author) values (?,?,?,?,?,?,?,?,?)'
                row_epoch = int(round(time.time() * 1000))
                row = [row_epoch,str(source),str(title),length,str(date),str(description),str(content),str(url),str(author)]

                try:
                   self.client.sql(insert_statment,query_args=row)
                except Exception as ex1:
                   print(ex1)

        except Exception as e:
            # print error (if any)
            print("Error : " + str(e))



def main():
    # creating object of NewsClient Class
    api = NewsClient()
    # calling function to get news
    #api.get_news_to_ignite(query = 'covid')
    api.get_news_to_ignite(query = 'covid',category = 'health')
    api.get_news_to_ignite(query = 'covid',category = 'science')
    api.get_news_to_ignite(query = 'covid',category = 'business')

if __name__ == "__main__":
    # calling main function
    main()
