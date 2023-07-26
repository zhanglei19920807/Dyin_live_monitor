from selenium import webdriver
import time
from selenium.webdriver import ChromeOptions
import json
from config_database import *
import datetime

data_base = Database()

def main():
    try:
        lives = data_base.query_sql('select * from live_list where is_expire = 1')
        for live in lives['path'].values:
            option = ChromeOptions()
            option.headless = True
            option.add_argument('--no-sandbox')
            option.add_argument('--disable-dev-shm-usage')
            browser = webdriver.Chrome(options=option)
            # 设置最大等待时长为 10秒
            browser.implicitly_wait(10)
            browser.get('https://live.douyin.com/')
            time.sleep(1)
            textElement = browser.find_element_by_class_name("R0xtz8q0")
            textElement.click()
            time.sleep(3)
            textElement = browser.find_element_by_class_name("web-login-scan-code__content__qrcode-wrapper__qrcode")
            # textElement = browser.find_elements_by_class_name("web-login-scan-code__content__qrcode-wrapper__qrcode")
            data_base.update_sql(
                'update live_list set qr_code="{}" where path = "{}"'.format(textElement.get_attribute("src"), live))
            x = 0
            while True:
                if len(browser.find_elements_by_class_name('R0xtz8q0')) == 0:
                    # data_base.update_sql('update live_list set qr_code="扫码成功" where path = "{}"'.format(live))
                    data_base.update_sql('''update live_list set qr_code="扫码成功",cookie='{}', is_expire=0 where path = "{}"'''.format(json.dumps(browser.get_cookies()).replace('%','%%'),live))
                    # data_base.update_sql('''update live_list set is_expire=0 where path = "{}"'''.format(live))
                    break
                else:
                    if x > 60:
                        data_base.update_sql('update live_list set qr_code="扫码失败" where path = "{}"'.format(live))
                        break
                    x += 1
                    time.sleep(1)
                    continue
            browser.quit()
    except Exception as e:
        now_day = datetime.datetime.now()
        with open(f'data/log_{now_day.strftime("%Y%m%d")}.txt', mode='a', encoding='utf-8') as f:
            f.write(str(now_day) + f" : main error -> {e} \n")
        browser.quit()
        return '123'

while True:
    main()
    time.sleep(30)
