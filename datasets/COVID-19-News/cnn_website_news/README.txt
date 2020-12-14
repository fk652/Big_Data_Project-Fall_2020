For this data collection, I was able to get all news articles from CNN.com (https://www.cnn.com/article/sitemap-2020-4.html is the URL for April 2020)

I was considering using BeautifulSoup to parse the webpage, but in terms of data collection and cleaning, it was very easy to simply copy and paste all content 
from pages November 2019 - November 2020 and remove the top 2 lines as well as the bottom lines that were not in the format 'date,new_headline'

I was able to get 13 text files, each containing a month of news data. 

To concatenate the text files all into one, use the command 'cat *.txt >> ../all_news.txt', which will merge 13 months of news into one files 

Once all 13 files are concatenated into one, you can use the all_news.txt file with the news_cleaner.py file to clean and add the source of the news headline