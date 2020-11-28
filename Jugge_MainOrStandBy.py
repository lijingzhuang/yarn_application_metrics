#encoding:utf-8
import json
import urllib3

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

'''Conversion time function'''
# 毫秒级转化为hour/minutes
def conver_time(allTime):

    hours = (allTime/(60*60*1000))
    return hours

'''get active resourcemanager yarn application metrics'''
def gat_yarn_application_metrics():
    active_resoucemanager = get_ative_resourcemanager('10.90.48.126.p10880.ipport.internal.mob.com',
                                                      '10.90.48.126.p10880.ipport.internal.mob.com')
    http = urllib3.PoolManager()
    url = 'http://{0}/ws/v1/cluster/apps'.format(active_resoucemanager)
    response = http.request('GET', url)
    data = json.loads(response.data).get('apps').get('app')
    length = len(data)
    for i in range(length):
        '''判断application状态'''
        application_state = data[i]['state']
        if application_state == 'RUNNING':
            '''获取状态为running的application metrics'''
            id_metric = data[i]['id']
            user_metric = data[i]['user']
            queue_metric = data[i]['queue']
            ram_metric = data[i]['allocatedMB']
            cpu_metric = data[i]['allocatedVCores']
            elapsed_time = data[i]['elapsedTime']
            runtime_metric = conver_time(elapsed_time)
            print(id_metric + " " + user_metric + " " + queue_metric + " "
                  + str(ram_metric) + " " + str(cpu_metric) + " " + str(runtime_metric))

if __name__ == '__main__':
    '''获取yarn application metrics'''
    gat_yarn_application_metrics()