import requests
from bs4 import BeautifulSoup
import time
import random
import re
import pandas as pd
from fake_useragent import UserAgent
import concurrent.futures

def news_for_japan():
    news_Link = []
    news_Title = []
    news_Type = []
    news_Date = []
    news_Time = []
    news_Content = []
    sleep_time = random.random()
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    res = requests.get(url=urls, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    for j in range(0, len(soup.select('div[class=box_2]'))):
        try:
            # 標題
            title = soup.select('div[class=box_2]')[j].select('h2 a')[0].text
            news_Title.append(re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', '', title))
            print(re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', '', title))

            # 網址
            link = soup.select('div[class=box_2]')[j].select('h2 a')[0]["href"]
            news_Link.append(link)

            # 取新聞類型
            news_type = soup.select('div[class=box_2]')[j].select('span[class=date]')[0].text.split()[0]
            news_Type.append(re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', '', news_type))
            print(re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', '', news_type))

            # 發布日期
            news_date = soup.select('div[class=box_2]')[j].select('span[class=date]')[0].text.split()[2]
            news_Date.append(news_date)
            print(news_date)

            # 發布時間
            news_time = soup.select('div[class=box_2]')[j].select('span[class=date]')[0].text.split()[3]
            news_Time.append(re.sub('[-_、【】。；：)(「」，.&+\n\t\r\u3000]', '', news_time))

            # 新聞內容
            page_res = requests.get(url=link, headers=headers)
            page_soap = BeautifulSoup(page_res.text, 'html.parser')

            for content in page_soap.select('div[class="story"]'):
                try:
                    news_Content.append(re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', ' ', content.text))
                except:
                    pass
            for content in page_soap.select('div[class="story lazyload"]'):
                try:
                    news_Content.append(re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', ' ', content.text))
                except:
                    pass

        except:
            pass

    dict_news = {'標題': news_Title, '類型': news_Type, '網址': news_Link, '發布日期': news_Date, '發布時間': news_Time,
                 '內容': news_Content}
    news_data.append(dict_news)

    time.sleep(sleep_time)

id_star = 0       #設置網頁頁數
id_end = 100

urls = ['https://www.ettoday.net/news_search/doSearch.php?keywords=日本&idx=2&page={}'.format(i) for i in range(id_star, id_end + 1)]
news_data = []
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:  # 進行多工處理
        executor.map(news_for_japan, urls)

    df_news_crawler = pd.dataFrame(news_data)
    df_news_crawler.to_csv("news_for_japan.csv", index=False)

except:
    df_news_crawler = pd.dataFrame(news_data)
    df_news_crawler.to_csv("news_for_japan.csv", index=False)