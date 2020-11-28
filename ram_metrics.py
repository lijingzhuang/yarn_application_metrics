#!/bin/bash
import urllib3
import json


'''Conversion time function'''

# 毫秒级转化为hour/minutes
def conver_time(allTime):

    minutes = (allTime/(60*1000))
    return minutes

'''get yarn resourcemanager main or standby'''
# get active yarn resourcemanager
def get_ative_resourcemanager(rm1, rm2):
    http = urllib3.PoolManager()
    rm_list = [rm1, rm2]
    for resourcemanager in rm_list:
        url = 'http://{0}/ws/v1/cluster/info'.format(resourcemanager)
        response = http.request('GET', url)
        data = json.loads(response.data)
        ha_state = data['clusterInfo']['haState']
        if ha_state == "ACTIVE":
            return resourcemanager

if __name__ == '__main__':
    '''此处使用api接口地址'''
    active_rm = get_ative_resourcemanager('10.90.48.126.p10880.ipport.internal.mob.com',
                                   '10.90.48.126.p10880.ipport.internal.mob.com')
    print(active_rm)
    url = 'http://{0}/ws/v1/cluster/apps'.format(active_rm)
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    # data = json.loads(response.data).get('apps').get('app')[0]
    # print(data)
    length = len(json.loads(response.data).get('apps').get('app'))
    # print(length)
    for i in range(length):
        data = json.loads(response.data).get('apps').get('app')[i]
        # print(data)
        state_app = data['state']
        if state_app == 'RUNNING':
            # print(state_app)
            '''Application ID,User,Queue,CPU Cores,RAM ,runtime'''

            id_metric = data['id']
            user_metric = data['user']
            queue_metric = data['queue']
            cpu_metric = data['allocatedVCores']
            ram_metric = data['allocatedMB']
            '''Obtain Application start_run_time'''
            elapsed_time = data['elapsedTime']
            runtime_metric = conver_time(elapsed_time)
            print(id_metric +" "+ user_metric +" "+ queue_metric + " " + str(ram_metric))