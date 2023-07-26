from sqlalchemy import create_engine
import pandas as pd

class Database:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://ai_live:DM7B5YF9gaM8Fhs@rm-wz9sow4534es4uw4z.mysql.rds.aliyuncs.com:3306/ai_live')
        # self.engine = create_engine('mysql+pymysql://root:password@192.168.41.12:3306/test')
    def query_sql(self,sql):
        df = pd.read_sql(sql, self.engine)
        return df

    def update_sql(self,sql):
        self.engine.execute(sql)

    def write_sql(self,data,table_name):
        data.to_sql(table_name, self.engine, index=False, if_exists='append')