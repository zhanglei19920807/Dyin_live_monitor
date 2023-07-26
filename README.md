# Dyin_live_monitor
实时多进程的监控多个抖音直播间，主要监控其弹幕

## 运行环境
selenium==3.141.0
gevent==21.12.0
flask
requests
multiprocess
schedule
sqlalchemy==1.4.7
pymysql
pandas==1.1.5
kafka-python==2.0.2

## 项目介绍
通过爬虫包selenium将抖音弹幕爬取，进行入库收集，其后可以使用gpt对话模型对抖音用户的弹幕进行实时的回复、对违规弹幕进行舆情监控等等操作。
