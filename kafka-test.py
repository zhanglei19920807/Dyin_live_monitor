import datetime

from kafka import KafkaConsumer, TopicPartition

from config_database import *
from data_service import *

Db = Database()


def insert_log_sql(row, status, ai_action, ai_action_info):
    Db.update_sql(f"""
    insert into ai_action set
    page_id = {row['page_id']},
    plan_id = {row['plan_id']},
    project_id = {row['project_id']},
    ai_action = {ai_action},
    ai_action_info = '{ai_action_info}',
    plan_info = '{json.dumps(row)}',
    status = {status},
    create_time = '{datetime.datetime.now()}'
    """)


def ai_tg(cost, roi, zhuce, pay_num, yesterday_=False):
    real_cpa = cost / zhuce if zhuce > 0 else cost
    pay_rate = pay_num / zhuce if zhuce > 0 else 0
    if yesterday_ and cost < 500 and real_cpa > 100:
        return False
    if 500 <= cost < 1000:
        if (real_cpa > 180 and pay_num < 1) or real_cpa > 200:
            return False
    elif 1000 <= cost < 1500:
        if (roi < 0.12 and pay_rate < 0.25) or real_cpa > 220:
            return False
    elif 1500 <= cost < 2000:
        if roi < 0.12 or real_cpa > 220:
            return False
    elif 2000 <= cost < 3000:
        if (roi < 0.16 and pay_rate < 0.25) or real_cpa > 220:
            return False
    elif cost >= 3000:
        if roi < 0.16 or real_cpa > 220:
            return False
    return True


def get_material_user(body):
    """
    这是第一步 获取通过素材获取引来的用户。
    :param video_id:传入video_id
    :return:
    """
    requestArray = {'apiKey': 'ai_page_d', 'appKey': '620f5217e4b011e8c3778d33', 'groupKey': 'default',
                    'body': body}

    res = DataService(requestArray, '/cell/open/export/select')
    data = res.send()
    if data:
        return data['data']['ret']
    else:
        return []


def get_plan_info(page_ids, project_id):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    body = json.dumps({"part_date": today,
                       "page_id": page_ids,
                       "project_id": project_id})
    df = get_material_user(body=body)
    return df


def handle_message(message):
    message_value = json.loads(message.value)
    if 'AIaotorun' in message_value.get('info_page_name', '') or 'AIaotorun' in message_value.get('page_name', ''):
        # if message_value.get('media_id',0) == 1 and message_value.get('os','') == 'android':
        if message_value['event_name'] in (
                'apk_active_v3', 'user_login_v3', 'apk_reg_v3', 'user_orders_v3', 'role_create_v3',
                'sign_up_v3') and message_value.get('info_page_id') != -1:
            if message_value['project_id'] == 7:
                page_ids_7.append(int(message_value['info_page_id']))
            elif message_value['project_id'] == 1:
                page_ids_1.append(int(message_value['info_page_id']))
        elif message_value.get('event_name') == 'media_plan_hour_rpt' and message_value.get('page_id') != -1:
            if message_value['project_id'] == 7:
                page_ids_7.append(int(message_value['page_id']))
            elif message_value['project_id'] == 1:
                page_ids_1.append(int(message_value['page_id']))
    else:
        pass


def control_plan(url, data, project_id):
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Project-ID': f'anfeng_adv_{project_id}'}
    response = requests.post(url=url, data=json.dumps(data), headers=headers)
    now_day = datetime.datetime.now()
    with open(f'data/ai_tg_log_{now_day.strftime("%Y%m%d")}.txt', mode='a') as f:
        f.write(f'{now_day} : control plan  -> {response.text} \n')
    return response.status_code


def consume_topic(start_partition, end_partition):
    kafka_setting = {
        'sasl_plain_username': 'alikafka_post-cn-nif2165r200w',
        'sasl_plain_password': 'RelruMjIUREMUio5eN6SOjeZEXO0ga3V',
        'bootstrap_servers': ["10.10.101.52:9092", "10.10.101.53:9092", "10.10.101.54:9092"],
        'topic_name': 'dwd_user_event_v3',
        'consumer_id': 'ads-fengtou-ai-v2'
    }

    conf = kafka_setting

    consumer = KafkaConsumer(bootstrap_servers=conf['bootstrap_servers'],
                             group_id=conf['consumer_id'],
                             session_timeout_ms=10000,
                             max_poll_records=1000,
                             enable_auto_commit=True,
                             )
    topic_partition_list = [TopicPartition(conf['topic_name'], x) for x in range(start_partition, end_partition + 1)]
    consumer.assign(topic_partition_list)
    for partition in topic_partition_list:
        # end_offset = consumer.offsets_for_times({partition:1682169490000})[partition][0] # 按时间戳获取offset
        end_offset = consumer.end_offsets([partition])[partition]  # 最新的offset
        consumer.seek(partition, end_offset)
    return consumer


if __name__ == '__main__':
    page_ids_1, page_ids_7 = [], []
    consumer = consume_topic(0, 11)
    now_t = time.time()
    for message in consumer:
        try:
            handle_message(message)
            if time.time() < now_t + 2:
                continue
            else:
                if not page_ids_1 and not page_ids_7:
                    plan_info = []
                else:
                    plan_info = []
                    if page_ids_1:
                        page_ids = list(set(page_ids_1))
                        plan_info += get_plan_info(page_ids, 1)
                    if page_ids_7:
                        page_ids = list(set(page_ids_7))
                        plan_info += get_plan_info(page_ids, 7)

                for row in plan_info:
                    if row['game_id'] != 234:
                        continue
                    flag = ai_tg(cost=row['page_cost'],
                                 roi=row['roi_1d'],
                                 zhuce=row['reg_num'],
                                 pay_num=row['pay_num_1d']
                                 )
                    if flag:
                        if row['budget'] and row['page_cost'] > row['budget'] - 50:
                            # 如果预算到了还能继续投放则提高预算
                            # print("提高预算")
                            data = {"api_token": "351b33587c5fdd93bd42ef7ac9995a28",
                                    "media_key": 1,  # 媒体是头条
                                    "plan_id": row['plan_id'],
                                    "type": "budget",
                                    "data": {
                                        "budget": row['budget'] + 1000
                                    }
                                    }
                            status_code = control_plan(
                                url="https://admin-fengtou.zhangyou.com/api/media/common/plan/edit",
                                data=data,
                                project_id=row['project_id'])
                            insert_log_sql(row, int(status_code), 1, json.dumps({"budget": row['budget'] + 1000}))

                    else:
                        if row["media_status"] in ("no_schedule", "disable"):
                            continue
                        else:
                            if row['roi_1d'] < 0.01:
                                flag_yesterday = False
                            else:
                                yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                                body = json.dumps({"part_date": yesterday,
                                                   "page_id": [row['page_id']],
                                                   "project_id": row['project_id']})
                                df = get_material_user(body=body)
                                if not df:
                                    flag_yesterday = True
                                else:
                                    flag_yesterday = ai_tg(cost=df[0]['page_cost'],
                                                           roi=df[0]['roi_1d'],
                                                           zhuce=df[0]['reg_num'],
                                                           pay_num=df[0]['pay_num_1d'],
                                                           yesterday_=True
                                                           )
                            if flag_yesterday:
                                data = {
                                    "api_token": "351b33587c5fdd93bd42ef7ac9995a28",
                                    "media_key": 1,
                                    "plan_id": row['plan_id'],
                                    "type": "schedule_exclude_today",
                                    "data": {}
                                }
                                status_code = control_plan(
                                    url="https://admin-fengtou.zhangyou.com/api/media/common/plan/edit",
                                    data=data,
                                    project_id=row['project_id'])
                                insert_log_sql(row, int(status_code), 0, "控停今日")
                            else:
                                data = {
                                    "api_token": "351b33587c5fdd93bd42ef7ac9995a28",
                                    "media_key": 1,
                                    "plan_id": row['plan_id'],
                                    "type": "opt_status",
                                    "data": {
                                        "opt_status": "disable"
                                    }
                                }
                                status_code = control_plan(
                                    url="https://admin-fengtou.zhangyou.com/api/media/common/plan/edit",
                                    data=data,
                                    project_id=row['project_id'])
                                insert_log_sql(row, int(status_code), 0, "关停计划")

                now_t = time.time()
                page_ids_1, page_ids_7 = [], []

        except Exception as e:
            now_day = datetime.datetime.now()
            with open(f'data/ai_tg_log_{now_day.strftime("%Y%m%d")}.txt', mode='a') as f:
                f.write(f'{now_day} : error  -> {e} \n')
            now_t = time.time()
            page_ids_1, page_ids_7 = [], []
            continue
