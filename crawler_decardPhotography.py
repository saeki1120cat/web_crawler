from selenium.webdriver import Chrome
import time

url = 'https://www.dcard.tw/f'

driver = Chrome('./chromedriver')

driver.get(url)

driver.find_element_by_tag_name('input').send_keys('攝影')
driver.find_element_by_xpath('//*[@id="__next"]/div[1]/div/div[1]/div/div/form/button[2]').click()

time.sleep(3)

driver.execute_script('var s = document.documentElement.scrollTop=5000')

time.sleep(3)

driver.execute_script('var s = document.documentElement.scrollTop=0')

time.sleep(3)

driver.execute_script('var s = document.documentElement.scrollTop=5000')

html = driver.execute_script("return document.getElementsByTagName('html')[0].outerHTML")
print(html)
