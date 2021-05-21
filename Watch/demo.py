from pyhive import hive
import pandas as pd

# 连接hive导入数据
cursor = hive.Connection(host='localhost', port=10000, database='szt', username='root').cursor()

sql = "select deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no " \
      "from dwd_szt_out_pd " \
      "where station = '深圳北' and deal_date >= '2018-09-01 08:15' and deal_date <= '2018-09-01 08:25'"

cursor.execute(sql)
data = cursor.fetchall()

df = pd.DataFrame(data, columns=['deal_date', 'close_date', 'card_no', 'deal_value', 'deal_type', 'company_name',
                                 'car_no', 'station', 'conn_mark', 'deal_money', 'equ_no'])

df.to_csv('data.csv')