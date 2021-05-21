import json
import pandas

df = []

with open('../Data/szt-data.jsons', mode='r') as fp:
    for i in range(1337):
        text = fp.readline()
        dic = json.loads(text)
        for data in dic['data']:
            tmp = []
            tmp.append(data.get('deal_date', None))
            tmp.append(data.get('close_date', None))
            tmp.append(data.get('card_no', None))
            tmp.append(data.get('deal_value', None))
            tmp.append(data.get('deal_type', None))
            tmp.append(data.get('company_name', None))
            tmp.append(data.get('car_no', None))
            tmp.append(data.get('station', None))
            tmp.append(data.get('conn_mark', None))
            tmp.append(data.get('deal_money', None))
            tmp.append(data.get('equ_no', None))
            df.append(tmp)

pd = pandas.DataFrame(df,
                      columns=['deal_date', 'close_date', 'card_no', 'deal_value', 'deal_type', 'company_name', 'car_no', 'station', 'conn_mark', 'deal_money', 'equ_no'])

pd = pd.sort_values('deal_date')

pd.to_csv('../Data/szt_data.csv', encoding='utf-8', index=False)

print('success!!!')