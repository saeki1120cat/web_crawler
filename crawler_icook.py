import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import time
import random
import concurrent.futures
import math
import re
import pandas as pd

def cook(url):
    import requests
    from bs4 import BeautifulSoup
    import re
    from fake_useragent import UserAgent
    import random,time
    import pandas as pd
    sleep_time = random.random()
    ua = UserAgent()
    headers = {'User-Agent':ua.random}   
    res = requests.get(url, headers = headers)
    soup = BeautifulSoup(res.text, 'html.parser') 
    if soup.select('div[class="container-fluid"]'):
        time.sleep(sleep_time)
    else:
        # 食材 (food_ingredient)/ 食材份數 (food_num)
        # 將食材及份數分開儲存
    
        food1 = soup.find_all("div", attrs={"class":["ingredient-name","ingredient-unit"]})

        food2 = []
        for j in food1:
            food2.append(j.text)

        food_ingredient = []
        food_num = []
        for k in range(len(food2)):
            if k == 0 or k%2 == 0: 
                food_ingredient.append(food2[k].strip())
            else:
                food_num.append(food2[k])
                   
        #名稱(food_name)
        try:
            food_name = soup.find("h1", attrs={"class":["title"]}).text.strip()
        except:
            food_name = "NAN"
        
        #菜單簡介(food_description)
        try:
            food_description = soup.find("section", attrs = {"class":["description"]}).text.replace("\n", "").replace("描述", "")
        except:
            food_description = "None"
        
        #圖片(food_photo)
        food_photo = soup.find("a", attrs={"class":["glightbox ratio-container ratio-container-4-3"]}).get("href")
    
    
        #時間(food_time)
        try :
            food_time = soup.find("div",attrs={"class":["time-info info-block"]}).text.replace("\n", "").replace("時間", "")
        except:
            food_time = "None"
        
            
        #製作方法(cooking_method)
        try :
            cooking = soup.find_all("li",attrs={"class":["recipe-details-step-item"]})

            cooking_method = ""

            for c in cooking :
                cook_step = c.get("id")
                cooking_method += cook_step
                cook_step_item = c.text.replace("\n", "")
                cooking_method += cook_step_item
                cooking_method +="\n"
        except :
            cooking_method = "None"
    
    
        #菜單作者(author_name) 
        author_name = soup.find("div", attrs = {"class":["author-name"]}).text.replace("\n","")
        

        dict_food =  {"菜單名稱": food_name, "圖片": food_photo, "料理時間": food_time, "食材":food_ingredient, "食材份數": food_num, "製作方法": cooking_method, "菜單簡介": food_description, "菜單作者": author_name}     
        
        icook_data.append(dict_food)

        time.sleep(sleep_time)

id_star = 0       #設置網頁頁數
id_end = 250000

urls = ['https://icook.tw/recipes/{}'.format(i) for i in range(id_star, id_end + 1)]

icook_data = [] 
try :    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor: #進行多工處理
        executor.map(cook, urls)
    
    icook_df = pd.DataFrame(icook_data)    
    icook_df.to_csv("icook.csv", index=False)
    
except:
    icook_df = pd.DataFrame(icook_data)    
    icook_df.to_csv("icook.csv", index=False)
