import time
import random
import hashlib
import json
import requests
import hashlib


class DataService:
    appSecret = '71c88c20-f447-4bb4-b268-f96d95ec6002'#正式
    baseUrl = 'http://export.dsapi2.zhangyou.com'# 正式
    # appSecret = '2329341d-99bf-4a40-8860-dd320bca334e'#测试
    # baseUrl = 'http://export.dsapi2.qcwanwan.com'# 测试
    def __init__(self, data,uri,type=1):
        self.data = data
        self.appSecret = DataService.appSecret
        self.public = {'code':'0','timestamp':str(int(time.time())*1000),
                       'nonceStr':''.join(random.sample('123456789zyxwvutsrqponmlkjihgfedcba', 32))}
        self.public['sign'] = self.sign({**self.data, **self.public})

        self.uri = uri
        self.type = type

        self.baseUrl = DataService.baseUrl

    def sign(self, sign_data):
        sign_string = self.get_sign(sign_data) + '&key=' + self.appSecret
        hl = hashlib.md5()
        hl.update(sign_string.encode(encoding='utf-8'))
        return (hl.hexdigest()).upper()

    def get_sign(self, body):
        list = []
        for i in body.items():
            if i[1] != "" and i[0] != "sign":
                list.append("=".join(i))
        sort = "&".join(sorted(list))
        result = sort
        return result

    def get_with_sign(self, body):
        list = []
        for i in body.items():
            if i[1] != "":
                list.append("=".join(i))
        sort = "&".join(list)
        result = sort
        return result

    def send(self):
        request = {**self.data, **self.public}
        body = json.loads(request['body'])
        request.pop('body')
        url = self.baseUrl + self.uri + '?' + self.get_with_sign(request)

        headers = {'Content-Type': 'application/json'}
        options = {}
        options = body
        response = requests.request("POST", url, headers=headers, data=json.dumps(options), verify=False)
        # resp = urllib.request.Request(url, data=urllib.urlencode(options), headers={'application': 'json'})
        res_text = response.text
        res_text = json.loads(res_text)

        if res_text['status'] == 200:
            if self.type != 1:
                return res_text
            return res_text




