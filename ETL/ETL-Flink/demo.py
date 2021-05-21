from pyhive import hive
import pandas as pd

# 连接hive导入数据
cursor = hive.Connection(host='localhost', port=10000, database='szt', username='root').cursor()

sql = "select card_no, deal_type, deal_date " \
      "from dwd_szt_in_out_pd " \
      "where deal_date <= '2018-09-01 08:25'"

cursor.execute(sql)
data = cursor.fetchall()

df = pd.DataFrame(data, columns=['card_no', 'deal_type', 'deal_date'])

res = 0
dic = dict()

for i in range(df.shape[0]):
    if df.iloc[i, 1] == '地铁入站':
        res += 1
        # if df.iloc[i, 0] in dic:
        #     print(df.iloc[i])
        dic[df.iloc[i, 0]] = df.iloc[i, 2]
    else:
        if df.iloc[i, 0] in dic:
            res -= 1
            dic.pop(df.iloc[i, 0])
        # else:
        #     print(df.iloc[i])

print(res)