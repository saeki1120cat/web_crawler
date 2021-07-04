import requests
from bs4 import BeautifulSoup
import csv
import random
import json
import re
import datetime
import pandas as pd
from fake_useragent import UserAgent

keyword_str =str(input("キーワード"))
country = str(input("勤務地"))
page_num = int(input("請問您想抓取幾頁"))

url_A ='https://jp.indeed.com/jobs?q='+ keyword_str + '&l='+ country +'&start='

job_name = []
job_url = []
job_company=[]
job_location=[]

for i in range(0,page_num+1):
    url = url_A + str(int(i)*10)
    url2 = url_A + str(int(i+1)*10)
    print(url)
    ua = UserAgent()
    headers = {'User-Agent':ua.random}#, 'referer':url2}
    
    sleep_time = random.random()
    
    res = requests.get(url, headers = headers)
    soup = BeautifulSoup(res.text, 'html.parser')   
    
    jobs_b = soup.find_all('h2',class_='title')                        #搜尋所有職缺       
    jobs_c = soup.find_all("span", attrs={"class":["company"]})
    jobs_d = soup.find_all("span", attrs={"class":["location accessible-contrast-color-location"]})

    for bi in jobs_b:
        job_name.append(bi.find('a',class_="jobtitle turnstileLink").text.replace("\n", ""))           #職缺內容
        job_url.append('https://jp.indeed.com' + bi.find('a').get('href'))

    for ci in jobs_c:
        job_company.append(ci.text.replace("\n", ""))
    
    for di in jobs_d:
        job_location.append(di.text.replace("\n", ""))
        
    time.sleep(sleep_time)
        
dict_job = {"職缺內容": job_name, "公司網址": job_url, "公司名稱": job_company, "公司地點": job_location}      

jobs_df = pd.DataFrame(dict_job)

jobs_df.to_csv("indeedjob.csv")
print("完成")
