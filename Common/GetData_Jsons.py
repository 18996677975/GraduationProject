import requests

se = requests.session()

appKey = '890c9657b6924107b677fe877b8bc41e'

with open('../Data/szt-data.jsons', mode='w') as fp:
    for i in range(1, 1338):
        print(i)
        http = 'https://opendata.sz.gov.cn/api/29200_00403601/1/service.xhtml?page=' + str(
            i) + '&rows=1000&appKey=' + appKey
        text = se.post(http).text
        fp.write(text + '\n')

print('success!!!')