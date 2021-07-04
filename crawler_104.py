import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import random,time
import json
import re
import datetime


today=datetime.date.today()
formatted_today=str(today.strftime('%y%m%d'))

keyword_str =str(input("請輸入關鍵字"))
page_num = int(input("請問您想抓取幾頁"))

keyword_str_r = keyword_str.replace(" ", "%20")
keyword_str_r2 = keyword_str.replace(" ", "_")

url_A ='https://www.104.com.tw/jobs/search/?'

all_job_datas=[]
for page in range(1,page_num+1):
    url = url_A +'keyword='+ keyword_str_r + '&page=' + str(page)
    print(url)
    ua = UserAgent()
    headers = {'User-Agent':ua.random}
    
    res = requests.get(url, headers = headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    jobs = soup.find_all('article',class_='js-job-item')                     #搜尋所有職缺
    
            
    
    for job in jobs:
        job_name=job.find('a',class_="js-job-link").text                    #職缺內容
        job_company=job.get('data-cust-name')                               #公司名稱
        #job_loc=job.find('ul', class_='job-list-intro').find('li').text     #地址
        job_pay=job.find('span',class_='b-tag--default').text               #薪資
        job_url='https:' + job.find('a').get('href')                        #網址
        
        
        job_url_num = re.findall("/[\w]*\?",job_url)
        ajax_url = 'https://www.104.com.tw/job/ajax/content' + job_url_num[0].strip('?') #處理ajax網址
        headers = {"Referer": job_url,}
        resB = requests.get(ajax_url, headers = headers)
        resB_json = json.loads(resB.text)
        
        job_edu = str(resB_json['data']['condition']['edu'])
        
        job_loc = str(resB_json['data']['jobDetail']['addressRegion'])+str(resB_json['data']['jobDetail']['addressDetail']) #地址
        
        job_Des = str(resB_json['data']['jobDetail']['jobDescription'])
        
        if resB_json['data']['condition']['specialty'] == []: #擅長工具
            job_specialty = '不拘'
        else:
            job_specialty=''
            for i in range(len(resB_json['data']['condition']['specialty'])):
                job_specialty +=str(i+1)
                job_specialty +='.'
                job_specialty +=str(resB_json['data']['condition']['specialty'][i]['description'])
                job_specialty +='\n'
        
        
        if resB_json['data']['condition']['skill'] == []:   #工作技能
            job_skill = '不拘'
        else:
            job_skill=''
            for i in range(len(resB_json['data']['condition']['skill'])):
                job_skill +=str(i+1)
                job_skill +='.'
                job_skill +=str(resB_json['data']['condition']['skill'][i]['description'])
                job_skill +='\n'
                
        job_welfare = str(resB_json['data']['welfare']['welfare'])
        
        job_data={'職缺內容':job_name, '公司名稱':job_company, '地址':job_loc, '薪資':job_pay, '福利制度':job_welfare, '工作內容':job_Des,'學歷要求':job_edu, '擅長工具':job_specialty, '工作技能':job_skill, '網址':job_url}
        all_job_datas.append(job_data)
    time.sleep(random.randint(2,6))


fn = keyword_str_r2 + '_' +'104職缺' + str(page_num) + '頁_'+ formatted_today + '.csv'                                              #取CSV檔名
columns_name=['職缺內容', '公司名稱', '地址', '薪資', '福利制度', '工作內容', '學歷要求', '擅長工具', '工作技能', '網址'] #第一欄的名稱
#df = pd.DataFrame(all_job_datas,columns=columns_name)
with open(fn,'w',newline='',errors='ignore') as csvFile:               #定義CSV的寫入檔,並且每次寫入完會換下一行
    dictWriter = csv.DictWriter(csvFile,fieldnames=columns_name)       #定義寫入器
    dictWriter.writeheader()
    for data in all_job_datas:
        dictWriter.writerow(data)
print(fn,'爬取完成!')
