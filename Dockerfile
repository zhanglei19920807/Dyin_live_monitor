FROM registry-vpc.cn-shenzhen.aliyuncs.com/anfeng/publish:ai-live_url_base

MAINTAINER govictory@dingtalk.com

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install -i https://mirrors.aliyun.com/pypi/simple --no-cache-dir -r requirements.txt

COPY . /usr/src/app

RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone  && chmod +x ./start.sh
#EXPOSE 8090

ENTRYPOINT ["./start.sh"]
