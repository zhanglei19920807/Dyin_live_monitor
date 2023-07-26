from selenium import webdriver
from selenium.webdriver import ChromeOptions
import time
import re
from multiprocessing.dummy import Process, Queue
from config_database import *
# import schedule
import datetime
import random
from selenium.webdriver.common.by import By
import requests
import pickle
import json
import os,signal
def handle_func(*args):
    # -1 表示监控任何子进程的退出
    # os.WNOHANG 如果没有立即可用的子进程状态，则立即返回 (0,0)
    cpid, status = os.waitpid(-1, os.WNOHANG)
signal.signal(signal.SIGCHLD, handle_func)
# dic = {
#     '领游戏码': '游戏码在主播粉丝群可以领取哦~', '领礼包码': '游戏兑换码在主播粉丝群可以领取哦~', '领兑换码': '游戏福利码在主播粉丝群可以领取哦~', '领福利码': '码在主播粉丝群可以领取哦~',
#     '领码': '游戏兑换码在主播粉丝群可以领取哦~', '哪里有码': '想要兑换码可以进粉丝群哦~', '码在哪里': '哥哥想要兑换码可以去粉丝群找管理员~', '有码吗': '左上角关注进入粉丝群可以看到码~',
#     '有没有码': '粉丝群可以领取兑换码喔~', '进群': '关注直播间，点击左上角黄色小爱心可以看到粉丝群入口！', '粉丝群': '关注直播间，点击左上角黄色小爱心可以看到粉丝群入口！',
#     '群在哪': '关注直播间，点击左上角黄色小爱心可以看到粉丝群入口！', '攻略': '粉丝群有主播推荐的攻略玩法，哥哥们可以参考一下！', '怎么玩': '粉丝群有主播推荐的攻略玩法，哥哥们可以参考一下！',
#     '怎么起号': '粉丝群有主播推荐的攻略玩法，哥哥们可以参考一下！', '材料去哪里打': '推荐哥哥们去回馈图打材料，粉丝群有攻略可以参考哦！', '材料不够': '推荐哥哥们去回馈图打材料，粉丝群有攻略可以参考哦！',
#     '元宝不够': '推荐哥哥们去回馈图打材料，粉丝群有攻略可以参考哦！', '元宝去哪里打': '推荐哥哥们去回馈图打材料，粉丝群有攻略可以参考哦！', '怎么下载': '点击右下角小手柄可以和主播玩同款游戏！单职业满攻速！',
#     '你玩的什么游戏': '主播玩的是怒战红颜，点击手柄可以玩同款游戏哦！', '这是什么游戏': '主播玩的是怒战红颜，点击手柄可以玩同款游戏哦！', '几个职业': '怒战红颜是战士单职业满攻速版本，点击手柄可以一起玩！',
#     '领不了码': '目前游戏内48小时只能领取三个码哦！', '码有限制': '目前游戏内48小时只能领取三个码哦！', '码过期了': '每个月码都会更新，哥哥们以粉丝群通知为准！',
#     '码不能领': '每个月码都会更新，哥哥们以粉丝群通知为准！'
# }

data_base = Database()


emji = ['♔','♕','♖','♚','♛','♜','☼','☽', '☾','❅','❆','★','☆','✦','✪','✫','✿','❀','❁','ღ','☚','☛','☜','☞','☟','♡','♢','♤','♧','★','☆','✦','✧','✩','✪','✫','✬','✭','✮','✯','✰']


def send(browser, word):
    while True:
        textElement = browser.find_elements_by_class_name('webcast-chatroom___textarea')
        if textElement:
            presend_word = textElement[0].get_attribute('value')
            if presend_word:
                time.sleep(2)
            textElement[0].clear()
            textElement[0].send_keys(word)  # 输入新字符串
            sendElement = browser.find_element(By.XPATH,
                                               '//button[@class="webcast-chatroom___send-btn"][@type="button"]')
            time.sleep(1)
            sendElement.click()
            break
        else:
            time.sleep(1)
            if len(browser.find_elements_by_class_name('YQXSUEUr')) >=1:
                break
            else:
                continue


def douyincrawler(path,browser):

    B = {}
    while True:
        try:
            status = browser.find_elements_by_class_name('YQXSUEUr')
            if len(status) >= 1:
                data_base.update_sql('update live_list set status=0 where path = "{}"'.format(path))
                browser.quit()
                return
            video_id_lists = list(set(
                re.findall(r'class="webcast-chatroom___content-with-emoji-text">(.*?)</span>', browser.page_source)))
            chatroom_list = list(set(re.findall(r'webcast-chatroom___item(.*?)</div></div>', browser.page_source)))
            A = {}
            if len(video_id_lists) > 0:
                for x in chatroom_list:
                    contente = re.findall(r'class="webcast-chatroom___content-with-emoji-text">(.*?)</span>', x)
                    data_id = re.findall(r'data-id="(.*?)"', x)[0]
                    if not data_id.isdigit():
                        continue
                    fang = x.find('webcast.douyinpic.com/img/webcast/webcast_admin_badge.png~tplv-obj.image')
                    if fang > -1:
                        continue
                    if len(contente) > 0:
                        A[data_id] = contente[0]
                        if data_id not in B.keys():
                            df = pd.DataFrame(
                                {'content': contente[0], 'path': path,
                                 'create_time': time.strftime("%Y-%m-%d %H:%M:%S"),
                                 'data_id': data_id}, index=[0])
                            data_base.write_sql(df, 'live_chat')
            B = A
            time.sleep(1)
        except Exception as e:
            now_day = datetime.datetime.now()
            with open(f'data/log_{now_day.strftime("%Y%m%d")}.txt', mode='a', encoding='utf-8') as f:
                f.write(str(now_day) + f" : status_crawler error -> {path} -> {e} \n")
            data_base.update_sql('update live_list set status=0 where path = "{}"'.format(path))
            browser.quit()
            return ''


def douYinAutoSend(path,browser):

    time.sleep(3)
    now = time.time()
    list_ = []
    i = 0
    while True:
        try:
            status = browser.find_elements_by_class_name('YQXSUEUr')
            if len(status) >= 1:
                data_base.update_sql('update live_list set status=0 where path = "{}"'.format(path))
                browser.quit()
                return
            auto_list = data_base.query_sql(f'select * from auto_send_list where status =1 and path ="{path}"')
            time_list = auto_list[auto_list['type'] == 2]
            auto_list = auto_list[auto_list['type'] == 1]
            now_yymmdd = (str(datetime.datetime.now()))
            for index, row in time_list.iterrows():
                if (now_yymmdd.split(' ')[0] == row['time'].split(' ')[0]) and (
                        now_yymmdd.split(' ')[1] >= row['time'].split(' ')[1]):
                    send(browser, random.choice(emji) + row['content'])
                    data_base.update_sql(f'update auto_send_list set status=2 where id={row["id"]}')

            for group in auto_list['group'].unique():
                list_.append(auto_list[auto_list['group'] == group]['content'].values)
            if time.time() >= now and list_:
                send(browser, random.choice(emji) + random.choice(list_[i]))
                now = now + random.randint(int(auto_list["min_interval"].values[0] / len(auto_list['group'].unique())),
                                           int(auto_list["max_interval"].values[0] / len(auto_list['group'].unique())))
                i = i + 1
            time.sleep(1)
        except Exception as e:
            now_day = datetime.datetime.now()
            with open(f'data/log_{now_day.strftime("%Y%m%d")}.txt', mode='a', encoding='utf-8') as f:
                f.write(str(now_day) + f" : status_send error -> {path} -> {e} \n")
            data_base.update_sql('update live_list set status=0 where path = "{}"'.format(path))
            browser.quit()
            return ''


def douYinrep(path,browser):

    time.sleep(3)
    while True:
        try:
            status = browser.find_elements_by_class_name('YQXSUEUr')
            if len(status) >= 1:
                data_base.update_sql('update live_list set status=0 where path = "{}"'.format(path))
                browser.quit()
                return
            chat_list = data_base.query_sql('select * from live_chat where type = 0 and path="{}"'.format(path))
            path_rep_list = data_base.query_sql(f'select * from ai_rep_list where status =1 and path = "{path}"')
            dic = path_rep_list.set_index(['question'])['answer'].to_dict()
            rep_list = []
            for row,chat in chat_list.iterrows():
                for an in dic.keys():
                    if an in chat['content'] and chat['content'] not in dic.values() and dic[an] not in rep_list:
                        send(browser,  random.choice(emji)+dic[an])
                        rep_list.append(dic[an])
                        time.sleep(3)
                        break
                data_base.update_sql('update live_chat set type=1 where data_id = "{}"'.format(chat['data_id']))

            time.sleep(1)
        except Exception as e:
            now_day = datetime.datetime.now()
            with open(f'data/log_{now_day.strftime("%Y%m%d")}.txt', mode='a', encoding='utf-8') as f:
                f.write(str(now_day) + f" : status_rep error -> {path} -> {e} \n")
            data_base.update_sql('update live_list set status=0 where path = "{}"'.format(path))

            browser.quit()
            return ''


def main():
    try:
        live_list = data_base.query_sql('select * from live_list where status = 0')
        for index,live in live_list.iterrows():
            option = ChromeOptions()
            option.add_argument(
                'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36"'
            )
            option.headless = True
            option.add_argument('--no-sandbox')
            option.add_argument('--disable-dev-shm-usage')
            option.add_experimental_option('excludeSwitches', ['enable-automation'])  # 防止系统检测到自动化工具
            option.add_experimental_option('useAutomationExtension', False)
            browser = webdriver.Chrome(options=option)
            browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
            })
            browser.get(live['path'])
            status = browser.find_elements_by_class_name('YQXSUEUr')
            if len(status) >= 1:
                browser.quit()
                continue
            else:
                cookies = data_base.query_sql('select cookie from live_list where path = "{}"'.format(live['path']))
                cookiesList = json.loads(cookies['cookie'].values[0])
                for cookie in cookiesList:
                    browser.add_cookie(cookie)
                browser.get(live['path'])
                time.sleep(1)
                if len(browser.find_elements_by_class_name('R0xtz8q0')) > 0:
                    data_base.update_sql('update live_list set is_expire=1 where path = "{}"'.format(live['path']))
                    browser.quit()
                    continue
                else:
                    data_base.update_sql('update live_list set is_expire=0 where path = "{}"'.format(live['path']))
                data_base.update_sql('update live_list set status=1 where path = "{}"'.format(live['path']))
                process_live = Process(target=douyincrawler, args=(live['path'],browser))
                process_auto = Process(target=douYinAutoSend, args=(live['path'],browser))
                process_rep = Process(target=douYinrep, args=(live['path'],browser))
                process_live.start()
                process_auto.start()
                process_rep.start()
    except Exception as e:
        now_day = datetime.datetime.now()
        with open(f'data/log_{now_day.strftime("%Y%m%d")}.txt', mode='a', encoding='utf-8') as f:
            f.write(str(now_day) + f" : main error -> {e} \n")
        return 123

data_base.update_sql('update live_list set status=0')
while True:
    main()
    time.sleep(15)
# schedule.every(10).seconds.do(main)
# while True:
#     schedule.run_pending()
#     time.sleep(1)
