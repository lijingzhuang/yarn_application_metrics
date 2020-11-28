import urllib3
import json


# get active namenode
def get_hdfs_namenode_state(namenode1, namenode2):
    http = urllib3.PoolManager()
    nn_list = [namenode1, namenode2]
    for namenode in nn_list:
        url = 'http://{0}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem'.format(namenode)
        response = http.request('GET', url)
        data = json.loads(response.data).get('beans')[0]
        ha_state = data['tag.HAState']
        if ha_state == "active":
            return namenode

# get active hbase master
def get_hbase_master_state(master1, master2):
    http = urllib3.PoolManager()
    nn_list = [master1, master2]
    for hmaster in nn_list:
        url = 'http://{0}/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server'.format(hmaster)
        response = http.request('GET', url)
        data = json.loads(response.data).get('beans')[0]
        ha_state = data['tag.isActiveMaster']
        if ha_state == "true":
            return hmaster

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

# hdfs pending delete blocks
def get_hdfs_pending_delete_blocks(nn):
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem'.format(nn)
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    hdfs_pending_deletion_blocks = data['PendingDeletionBlocks']
    print(hdfs_pending_deletion_blocks)


# hdfs namenode rpc8040 processing平均时间
def get_namenode_rpc8040_processing_avgtime(nn):
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=NameNode,name=RpcActivityForPort8040'.format(nn)
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    nn_rpc8040_processing_time_avgtime = data['RpcProcessingTimeAvgTime']
    print(nn_rpc8040_processing_time_avgtime)


# hdfs namenode rpc8020 processing平均时间
def get_namenode_rpc8020_processing_avgtime(nn):
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=NameNode,name=RpcActivityForPort8020'.format(nn)
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    nn_rpc8020_processing_time_avgtime = data['RpcProcessingTimeAvgTime']
    print(nn_rpc8020_processing_time_avgtime)


# hdfs dead datanode num
def get_num_dead_datanodes(nn):
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState'.format(nn)
    #print url
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    # print data
    num_dead_datanodes = data['NumDeadDataNodes']
    print(num_dead_datanodes)


# hbase regionserver dead num
def get_num_dead_regionservers(hmaster):
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server'.format(hmaster)
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    num_dead_regionservers = data['numDeadRegionServers']
    print(num_dead_regionservers)


# hdfs datanode 坏盘检测
def get_num_datanode_failed_storage(nn):
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo'.format(nn)
    response = http.request('GET', url)
    # print response
    data = json.loads(response.data).get('beans')[0]
    # print data
    live_nodes = data['LiveNodes']
    dic_nodes = json.loads(live_nodes)
    failed_storage = {}
    for k, v in dic_nodes.items():
        if 'failedStorageLocations' in v.keys():
            # failed_storage = {}
            # print k, v['failedStorageLocations']
            failed_storage[k] = v['failedStorageLocations']
    # print failed_storage
    print(len(failed_storage))

# yarn total vcore and memery
def get_yarn_resource_metric(rm):
    # print("get args:" + rm)
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root'.format(rm)
    # print url
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    # print data
    total_vcores = data['SteadyFairShareVCores']
    # print total_vcores
    total_mems = data['SteadyFairShareMB']
    # print total_mems
    used_vcores = data['AllocatedVCores']
    # print used_vcores
    # print response
    data = json.loads(response.data).get('beans')[0]
    # print data
    live_nodes = data['LiveNodes']
    dic_nodes = json.loads(live_nodes)
    failed_storage = {}
    for k, v in dic_nodes.items():
        if 'failedStorageLocations' in v.keys():
            # failed_storage = {}
            # print k, v['failedStorageLocations']
            failed_storage[k] = v['failedStorageLocations']
    # print failed_storage
    print(len(failed_storage))

# yarn total vcore and memery
def get_yarn_resource_metric(rm):
    # print("get args:" + rm)
    http = urllib3.PoolManager()
    url = 'http://{0}/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root'.format(rm)
    print(url)
    response = http.request('GET', url)
    data = json.loads(response.data).get('beans')[0]
    # print data
    total_vcores = data['SteadyFairShareVCores']
    # print total_vcores
    total_mems = data['SteadyFairShareMB']
    # print total_mems
    used_vcores = data['AllocatedVCores']
    # print used_vcores
    used_mems = data['AllocatedMB']
    # print used_mems
    # used_vcore_percentage = used_vcores / total_vcores * 100
    # used_mem_percentage = used_mems / total_mems * 100
    print(total_vcores)
    print(total_mems)
    print(used_vcores)
    print(used_mems)


# main
if __name__ == '__main__':
    # 134.64.14.230:50070 134.64.14.231:50070
    # 26 cluster
    # hdfs
    # nn = get_hdfs_namenode_state('10.90.48.127.p50070.ipport.internal.mob.com', '10.90.48.126.p50070.ipport.internal.mob.com')
    # get_hdfs_pending_delete_blocks(nn)
    # get_namenode_rpc8040_processing_avgtime(nn)
    # get_namenode_rpc8020_processing_avgtime(nn)
    # get_num_datanode_failed_storage(nn)
    #
    # # 55 data cloud
    # nn = get_hdfs_namenode_state('10.21.34.175.p50070.ipport.internal.mob.com','10.21.34.174.p50070.ipport.internal.mob.com')
    # get_num_dead_datanodes(nn)
    # get_num_datanode_failed_storage(nn)
    #
    # hmaster = get_hbase_master_state('10.21.34.175.p60010.ipport.internal.mob.com','10.21.34.174.p60010.ipport.internal.mob.com')
    # get_num_dead_regionservers(hmaster)

    # 26 yarn
    rm = get_ative_resourcemanager('10.90.48.126.p10880.ipport.internal.mob.com', '10.90.48.127.p10880.ipport.internal.mob.com')
    #print(rm)
    get_yarn_resource_metric(rm)
