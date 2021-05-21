from pyhive import hive
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# 连接hive导入数据
cursor = hive.Connection(host='localhost', port=10000, database='szt', username='root').cursor()

# sql = "select case           " \
#       "when deal_date <= '2018-09-01 06:00' then '06:00'           " \
#       "when deal_date <= '2018-09-01 06:05' then '06:05'           " \
#       "when deal_date <= '2018-09-01 06:10' then '06:10'           " \
#       "when deal_date <= '2018-09-01 06:15' then '06:15'           " \
#       "when deal_date <= '2018-09-01 06:20' then '06:20'           " \
#       "when deal_date <= '2018-09-01 06:25' then '06:25'           " \
#       "when deal_date <= '2018-09-01 06:30' then '06:30'           " \
#       "when deal_date <= '2018-09-01 06:35' then '06:35'           " \
#       "when deal_date <= '2018-09-01 06:40' then '06:40'           " \
#       "when deal_date <= '2018-09-01 06:45' then '06:45'           " \
#       "when deal_date <= '2018-09-01 06:50' then '06:50'           " \
#       "when deal_date <= '2018-09-01 06:55' then '06:55'           " \
#       "when deal_date <= '2018-09-01 07:00' then '07:00'           " \
#       "when deal_date <= '2018-09-01 07:05' then '07:05'           " \
#       "when deal_date <= '2018-09-01 07:10' then '07:10'           " \
#       "when deal_date <= '2018-09-01 07:15' then '07:15'           " \
#       "when deal_date <= '2018-09-01 07:20' then '07:20'           " \
#       "when deal_date <= '2018-09-01 07:25' then '07:25'           " \
#       "when deal_date <= '2018-09-01 07:30' then '07:30'           " \
#       "when deal_date <= '2018-09-01 07:35' then '07:35'           " \
#       "when deal_date <= '2018-09-01 07:40' then '07:40'           " \
#       "when deal_date <= '2018-09-01 07:45' then '07:45'           " \
#       "when deal_date <= '2018-09-01 07:50' then '07:50'           " \
#       "when deal_date <= '2018-09-01 07:55' then '07:55'           " \
#       "when deal_date <= '2018-09-01 08:00' then '08:00'           " \
#       "when deal_date <= '2018-09-01 08:05' then '08:05'           " \
#       "when deal_date <= '2018-09-01 08:10' then '08:10'           " \
#       "when deal_date <= '2018-09-01 08:15' then '08:15'           " \
#       "when deal_date <= '2018-09-01 08:20' then '08:20'           " \
#       "when deal_date <= '2018-09-01 08:25' then '08:25'           " \
#       "when deal_date <= '2018-09-01 08:30' then '08:30'           " \
#       "when deal_date <= '2018-09-01 08:35' then '08:35'           " \
#       "when deal_date <= '2018-09-01 08:40' then '08:40'           " \
#       "when deal_date <= '2018-09-01 08:45' then '08:45'           " \
#       "when deal_date <= '2018-09-01 08:50' then '08:50'           " \
#       "when deal_date <= '2018-09-01 08:55' then '08:55'           " \
#       "when deal_date <= '2018-09-01 09:00' then '09:00'           " \
#       "when deal_date <= '2018-09-01 09:05' then '09:05'           " \
#       "when deal_date <= '2018-09-01 09:10' then '09:10'           " \
#       "when deal_date <= '2018-09-01 09:15' then '09:15'           " \
#       "when deal_date <= '2018-09-01 09:20' then '09:20'           " \
#       "when deal_date <= '2018-09-01 09:25' then '09:25'           " \
#       "when deal_date <= '2018-09-01 09:30' then '09:30'           " \
#       "when deal_date <= '2018-09-01 09:35' then '09:35'           " \
#       "when deal_date <= '2018-09-01 09:40' then '09:40'           " \
#       "when deal_date <= '2018-09-01 09:45' then '09:45'           " \
#       "when deal_date <= '2018-09-01 09:50' then '09:50'           " \
#       "when deal_date <= '2018-09-01 09:55' then '09:55'           " \
#       "when deal_date <= '2018-09-01 10:00' then '10:00'           " \
#       "when deal_date <= '2018-09-01 10:05' then '10:05'           " \
#       "when deal_date <= '2018-09-01 10:10' then '10:10'           " \
#       "when deal_date <= '2018-09-01 10:15' then '10:15'           " \
#       "when deal_date <= '2018-09-01 10:20' then '10:20'           " \
#       "when deal_date <= '2018-09-01 10:25' then '10:25'           " \
#       "when deal_date <= '2018-09-01 10:30' then '10:30'           " \
#       "when deal_date <= '2018-09-01 10:35' then '10:35'           " \
#       "when deal_date <= '2018-09-01 10:40' then '10:40'           " \
#       "when deal_date <= '2018-09-01 10:45' then '10:45'           " \
#       "when deal_date <= '2018-09-01 10:50' then '10:50'           " \
#       "when deal_date <= '2018-09-01 10:55' then '10:55'           " \
#       "when deal_date <= '2018-09-01 11:00' then '11:00'           " \
#       "when deal_date <= '2018-09-01 11:05' then '11:05'           " \
#       "when deal_date <= '2018-09-01 11:10' then '11:10'           " \
#       "when deal_date <= '2018-09-01 11:15' then '11:15'           " \
#       "when deal_date <= '2018-09-01 11:20' then '11:20'           " \
#       "when deal_date <= '2018-09-01 11:25' then '11:25'           " \
#       "when deal_date <= '2018-09-01 11:30' then '11:30'           " \
#       "when deal_date <= '2018-09-01 11:35' then '11:35'           " \
#       "when deal_date <= '2018-09-01 11:40' then '11:40'           " \
#       "when deal_date <= '2018-09-01 11:45' then '11:45'           " \
#       "when deal_date <= '2018-09-01 11:50' then '11:50'           " \
#       "when deal_date <= '2018-09-01 11:55' then '11:55'           " \
#       "else 'other'           " \
#       "end                " \
#       "as times,       " \
#       "count(1) as num " \
#       "from dwd_szt_in_pd  " \
#       "group by case             " \
#       "when deal_date <= '2018-09-01 06:00' then '06:00'             " \
#       "when deal_date <= '2018-09-01 06:05' then '06:05'             " \
#       "when deal_date <= '2018-09-01 06:10' then '06:10'             " \
#       "when deal_date <= '2018-09-01 06:15' then '06:15'             " \
#       "when deal_date <= '2018-09-01 06:20' then '06:20'             " \
#       "when deal_date <= '2018-09-01 06:25' then '06:25'             " \
#       "when deal_date <= '2018-09-01 06:30' then '06:30'             " \
#       "when deal_date <= '2018-09-01 06:35' then '06:35'             " \
#       "when deal_date <= '2018-09-01 06:40' then '06:40'             " \
#       "when deal_date <= '2018-09-01 06:45' then '06:45'             " \
#       "when deal_date <= '2018-09-01 06:50' then '06:50'             " \
#       "when deal_date <= '2018-09-01 06:55' then '06:55'             " \
#       "when deal_date <= '2018-09-01 07:00' then '07:00'             " \
#       "when deal_date <= '2018-09-01 07:05' then '07:05'             " \
#       "when deal_date <= '2018-09-01 07:10' then '07:10'             " \
#       "when deal_date <= '2018-09-01 07:15' then '07:15'             " \
#       "when deal_date <= '2018-09-01 07:20' then '07:20'             " \
#       "when deal_date <= '2018-09-01 07:25' then '07:25'             " \
#       "when deal_date <= '2018-09-01 07:30' then '07:30'             " \
#       "when deal_date <= '2018-09-01 07:35' then '07:35'             " \
#       "when deal_date <= '2018-09-01 07:40' then '07:40'             " \
#       "when deal_date <= '2018-09-01 07:45' then '07:45'             " \
#       "when deal_date <= '2018-09-01 07:50' then '07:50'             " \
#       "when deal_date <= '2018-09-01 07:55' then '07:55'             " \
#       "when deal_date <= '2018-09-01 08:00' then '08:00'             " \
#       "when deal_date <= '2018-09-01 08:05' then '08:05'             " \
#       "when deal_date <= '2018-09-01 08:10' then '08:10'             " \
#       "when deal_date <= '2018-09-01 08:15' then '08:15'             " \
#       "when deal_date <= '2018-09-01 08:20' then '08:20'             " \
#       "when deal_date <= '2018-09-01 08:25' then '08:25'             " \
#       "when deal_date <= '2018-09-01 08:30' then '08:30'             " \
#       "when deal_date <= '2018-09-01 08:35' then '08:35'             " \
#       "when deal_date <= '2018-09-01 08:40' then '08:40'             " \
#       "when deal_date <= '2018-09-01 08:45' then '08:45'             " \
#       "when deal_date <= '2018-09-01 08:50' then '08:50'             " \
#       "when deal_date <= '2018-09-01 08:55' then '08:55'             " \
#       "when deal_date <= '2018-09-01 09:00' then '09:00'             " \
#       "when deal_date <= '2018-09-01 09:05' then '09:05'             " \
#       "when deal_date <= '2018-09-01 09:10' then '09:10'             " \
#       "when deal_date <= '2018-09-01 09:15' then '09:15'             " \
#       "when deal_date <= '2018-09-01 09:20' then '09:20'             " \
#       "when deal_date <= '2018-09-01 09:25' then '09:25'             " \
#       "when deal_date <= '2018-09-01 09:30' then '09:30'             " \
#       "when deal_date <= '2018-09-01 09:35' then '09:35'             " \
#       "when deal_date <= '2018-09-01 09:40' then '09:40'             " \
#       "when deal_date <= '2018-09-01 09:45' then '09:45'             " \
#       "when deal_date <= '2018-09-01 09:50' then '09:50'             " \
#       "when deal_date <= '2018-09-01 09:55' then '09:55'             " \
#       "when deal_date <= '2018-09-01 10:00' then '10:00'             " \
#       "when deal_date <= '2018-09-01 10:05' then '10:05'             " \
#       "when deal_date <= '2018-09-01 10:10' then '10:10'             " \
#       "when deal_date <= '2018-09-01 10:15' then '10:15'             " \
#       "when deal_date <= '2018-09-01 10:20' then '10:20'             " \
#       "when deal_date <= '2018-09-01 10:25' then '10:25'             " \
#       "when deal_date <= '2018-09-01 10:30' then '10:30'             " \
#       "when deal_date <= '2018-09-01 10:35' then '10:35'             " \
#       "when deal_date <= '2018-09-01 10:40' then '10:40'             " \
#       "when deal_date <= '2018-09-01 10:45' then '10:45'             " \
#       "when deal_date <= '2018-09-01 10:50' then '10:50'             " \
#       "when deal_date <= '2018-09-01 10:55' then '10:55'             " \
#       "when deal_date <= '2018-09-01 11:00' then '11:00'             " \
#       "when deal_date <= '2018-09-01 11:05' then '11:05'             " \
#       "when deal_date <= '2018-09-01 11:10' then '11:10'             " \
#       "when deal_date <= '2018-09-01 11:15' then '11:15'             " \
#       "when deal_date <= '2018-09-01 11:20' then '11:20'             " \
#       "when deal_date <= '2018-09-01 11:25' then '11:25'             " \
#       "when deal_date <= '2018-09-01 11:30' then '11:30'             " \
#       "when deal_date <= '2018-09-01 11:35' then '11:35'             " \
#       "when deal_date <= '2018-09-01 11:40' then '11:40'             " \
#       "when deal_date <= '2018-09-01 11:45' then '11:45'             " \
#       "when deal_date <= '2018-09-01 11:50' then '11:50'             " \
#       "when deal_date <= '2018-09-01 11:55' then '11:55'             " \
#       "else 'other'             " \
#       "end " \
#       "order by times"

sql = "select substr(deal_date, 12, 8), " \
      "count(1) as num " \
      "from dwd_szt_in_pd " \
      "group by deal_date"

cursor.execute(sql)
data = cursor.fetchall()

df = pd.DataFrame(data, columns=['times', 'num'])
df['index'] = df.index

# trainData = df.iloc[:-2]
# testData = df.iloc[-7:]

# 画图，原数据展示
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

plt.figure(figsize=(16, 8))
plt.plot(df['times'], df['num'])
plt.xlim(-1, 19098)
new_ticks = np.linspace(-1, 19098, 20)
plt.xticks(new_ticks)
plt.show()

# Regressors = [
#     ["RandomForest", RandomForestRegressor()]
#     , ["DecisionTree", DecisionTreeRegressor()]
#     , ["AdaBoostRegressor", AdaBoostRegressor()]
#     , ["XGB", XGBRegressor()]
#     , ["CatBoost", CatBoostRegressor(logging_level='Silent')]
# ]
#
# reg_result = []
# names = []
# prediction = []
#
# for name, reg in Regressors:
#     reg = reg.fit(trainData[['index']], trainData['num'])
#     pred = reg.predict(testData[['index']])
#     testData[name] = pred
#
#     # 回归评估
#     mae = mean_absolute_error(testData['num'], pred)
#     mse = mean_squared_error(testData['num'], pred)
#     r2 = r2_score(testData['num'], pred)
#     class_eva = pd.DataFrame([mae, mse, r2])
#     reg_result.append(class_eva)
#     name = pd.Series(name)
#     names.append(name)
#     pred = pd.Series(pred)
#     prediction.append(pred)
#
# # ARIMA
# fit = sm.tsa.statespace.SARIMAX(trainData['num'], order=(10, 2, 1), seasonal_order=(0, 1, 1, 5)).fit()
# testData['ARIMA'] = fit.predict(start=testData.index.to_list()[0], end=testData.index.to_list()[-1])
#
# mae = mean_absolute_error(testData['num'], testData['ARIMA'])
# mse = mean_squared_error(testData['num'], testData['ARIMA'])
# r2 = r2_score(testData['num'], testData['ARIMA'])
# class_eva = pd.DataFrame([mae, mse, r2])
# reg_result.append(class_eva)
# name = pd.Series('ARIMA')
# names.append(name)
# pred = pd.Series(testData['ARIMA'])
# prediction.append(pred)
#
# names = pd.DataFrame(names)
# names = names[0].tolist()
# result = pd.concat(reg_result, axis=1)
# result.columns = names
# result.index = ["mae", "mse", "r2"]
# result.to_csv('result.csv', index=True)
#
# plt.figure(figsize=(16, 8))
# plt.plot(trainData['times'], trainData['num'], label='Train')
# plt.plot(testData['times'], testData['num'], label='Test')
# plt.plot(testData['times'], testData['RandomForest'], label='RandomForest')
# plt.plot(testData['times'], testData['DecisionTree'], label='DecisionTree')
# plt.plot(testData['times'], testData['AdaBoostRegressor'], label='AdaBoostRegressor')
# plt.plot(testData['times'], testData['XGB'], label='XGB')
# plt.plot(testData['times'], testData['CatBoost'], label='CatBoost')
# plt.plot(testData['times'], testData['ARIMA'], label='ARIMA')
# plt.legend()
# plt.xlim(-1, 67)
# new_ticks = np.linspace(-1, 67, 20)
# plt.xticks(new_ticks)
# plt.show()