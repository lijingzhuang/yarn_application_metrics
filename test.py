import urllib3
import json
import time
import re

'''Conversion time function'''


# 毫秒级转化为day/hour/minutes/seconds
def conver_time(allTime):

    hour = (allTime/(60*60*1000))
    return hour

if __name__ == '__main__':
    url = 'http://10.90.48.126.p10880.ipport.internal.mob.com/ws/v1/cluster/apps'.format()
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    data = json.loads(response.data).get('apps').get('app')

    length = len(json.loads(response.data).get('apps').get('app'))
    for i in range(length):
        state = data[i]['state']
        '''筛选出Streaming的任务样'''
        app_name = data[i]['name']
        Streaming_match = re.findall(r'Stream', app_name, re.IGNORECASE)
        if state == 'RUNNING':
            '''找出非Stream任务并取值'''
            if not Streaming_match:

                id_metric = data[i]['id']
                user_metric = data[i]['user']
                queue_metric = data[i]['queue']
                ram_metric = data[i]['allocatedMB']
                print(id_metric + " " + user_metric + " " + queue_metric + " " + str(ram_metric))



