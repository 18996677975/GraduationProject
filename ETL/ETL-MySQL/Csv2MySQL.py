import pandas as pd
import pymysql

# 读取数据
data = pd.read_csv('//Data/szt_data.csv')

# 连接数据库
connect = pymysql.connect(host='localhost', port=3306, user='root', password='', charset='utf8')

# 得到一个可以执行SQL语句的光标对象
cursor = connect.cursor()

# 创建数据库
createDataBase = 'create database if not exists SZTDataBase;'
cursor.execute(createDataBase)

# 创建表
createTable = "create table if not exists SZTDataBase.SZTData " \
              "(" \
              "deal_date datetime comment '交易日期时间'," \
              "close_date datetime comment '结算日期'," \
              "card_no nchar(10) comment '卡号'," \
              "deal_value int comment '交易值'," \
              "deal_type nchar(10) comment '交易类型'," \
              "company_name nchar(40) comment '公司名称'," \
              "car_no nchar(20) comment '车牌号'," \
              "station nchar(100) comment '线路站点'," \
              "conn_mark int comment '联程标记'," \
              "deal_money int comment '交易金额'," \
              "equ_no nchar(10) comment '设备编码'" \
              ");"
cursor.execute(createTable)

for i in range(data.shape[0]):
    print(i)
    deal_date = "'" + data.iloc[i][0] + "'" if pd.notna(data.iloc[i][0]) else 'null'
    close_date = "'" + data.iloc[i][1] + "'" if pd.notna(data.iloc[i][1]) else 'null'
    card_no = "'" + data.iloc[i][2] + "'" if pd.notna(data.iloc[i][2]) else 'null'
    deal_value = data.iloc[i][3] if pd.notna(data.iloc[i][3]) else 'null'
    deal_type = "'" + data.iloc[i][4] + "'" if pd.notna(data.iloc[i][4]) else 'null'
    company_name = "'" + data.iloc[i][5] + "'" if pd.notna(data.iloc[i][5]) else 'null'
    car_no = "'" + data.iloc[i][6] + "'" if pd.notna(data.iloc[i][6]) else 'null'
    station = "'" + data.iloc[i][7] + "'" if pd.notna(data.iloc[i][7]) else 'null'
    conn_mark = data.iloc[i][8] if pd.notna(data.iloc[i][8]) else 'null'
    deal_money = data.iloc[i][9] if pd.notna(data.iloc[i][9]) else 'null'
    equ_no = "'" + str(data.iloc[i][10]) + "'" if pd.notna(data.iloc[i][10]) else 'null'
    insertData = "insert into SZTDataBase.SZTData (deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no) " \
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);" % \
                 (deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no)
    cursor.execute(insertData)

connect.commit()

cursor.close()
connect.close()

print('success!!!')