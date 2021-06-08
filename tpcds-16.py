from common import *
from config_vars import *
import lithops
import time

scale = 10
parall_1 = 10
parall_2 = 10
parall_3 = 10
# storage_mode = 'local'
storage_mode = 's3-only'
# storage_mode = 's3-redis'
# execution_mode = 'local'
execution_mode = 'lambda'
pywren_rate = 1000


query_name = "16"

n_nodes = len(HOSTNAMES)
instance_type = "cache.r3.8xlarge"

wrenexec = lithops.FunctionExecutor(runtime=RUNTIME)

stage_info_load = {}
stage_info_filename = "stage_info_load_" + query_name + ".pickle"
if os.path.exists(stage_info_filename):
    stage_info_load = pickle.load(open(stage_info_filename, "r"))

pm = [str(parall_1), str(parall_2), str(parall_3), str(pywren_rate), str(n_nodes)]
filename = "nomiti.cluster-" + storage_mode + '-tpcds-q' + query_name + '-scale' + str(scale) + "-" + "-".join(
    pm) + "-b" + str(N_BUCKETS) + ".pickle"

print("Scale is " + str(scale))

if storage_mode == 'local':
    temp_address = "/Users/qifan/data/q" + query_name + "-temp/"
else:
    temp_address = "scale" + str(scale) + "/q" + query_name + "-temp/"


def execute_lambda_stage(stage_function, tasks):
    t0 = time.time()
    for task in tasks:
        task['key']['write_output'] = True
    futures = wrenexec.map(stage_function, tasks)
    results = wrenexec.get_result(futures)
    t1 = time.time()
    res = {'results': results,
           't0': t0,
           't1': t1}
    return res


def execute_local_stage(stage_function, tasks):
    stage_info = []
    count = 0
    for task in tasks:
        print(count)
        count += 1
        task['write_output'] = True
        stage_info.append(stage_function(task))
    res = {'results': stage_info}
    return res


def execute_stage(stage_function, tasks):
    if execution_mode == 'local':
        res = execute_local_stage(stage_function, tasks)
    else:
        res = execute_lambda_stage(stage_function, tasks)

    for rr in res['results']:
        if rr['info']['outputs_info'] != '':
            rr['info']['outputs_info'] = pickle.loads(rr['info']['outputs_info'])
    return res


# implementing all stages

def stage1(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()
    output_address = key['output_address']
    cs = read_table(key)
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()
    wanted_columns = ['cs_order_number',
                      'cs_ext_ship_cost',
                      'cs_net_profit',
                      'cs_ship_date_sk',
                      'cs_ship_addr_sk',
                      'cs_call_center_sk',
                      'cs_warehouse_sk']
    cs_s = cs[wanted_columns]

    t1 = time.time()
    tc += t1 - t0

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cs_s, ['cs_order_number'], 'uniform', parall_1, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    info['outputs_info'] = outputs_info
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]

    return results


def stage2(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cr = read_table(key)

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cr, ['cr_order_number'], 'uniform', parall_1, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    info['outputs_info'] = outputs_info
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]
    return results


def stage3(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    # print(key['names'])
    # print(key['dtypes'])
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    cr = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])

    d = read_table(key['date_dim'])
    # return 1
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    cs_succient = cs[['cs_order_number', 'cs_warehouse_sk']]
    '''
    cs_sj = pd.merge(cs, cs_succient, on=['cs_order_number'])
    del cs
    del cs_succient
    cs_sj_f1 = cs_sj[cs_sj.cs_warehouse_sk_x != cs_sj.cs_warehouse_sk_y]
    del cs_sj
    cs_sj_f1.drop_duplicates(subset=cs_sj_f1.columns[:-1], inplace=True)
    '''
    # the above impl eats too much memory
    # trying an alternative
    #
    wh_uc = cs_succient.groupby(['cs_order_number']).agg({'cs_warehouse_sk': 'nunique'})
    target_order_numbers = wh_uc.loc[wh_uc['cs_warehouse_sk'] > 1].index.values
    cs_sj_f1 = cs.loc[cs['cs_order_number'].isin(target_order_numbers)]

    cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['cs_order_number'].isin(cr.cr_order_number)]
    del cs_sj_f1
    # cs_sj_f2.rename(columns = {'cs_warehouse_sk_y':'cs_warehouse_sk'}, inplace = True)

    # join date_dim
    dd = d[['d_date', 'd_date_sk']]
    dd_select = dd[(pd.to_datetime(dd['d_date']) > pd.to_datetime('2002-02-01')) & (
                pd.to_datetime(dd['d_date']) < pd.to_datetime('2002-04-01'))]
    dd_filtered = dd_select[['d_date_sk']]

    merged = cs_sj_f2.merge(dd_filtered, left_on='cs_ship_date_sk', right_on='d_date_sk')
    del dd
    del cs_sj_f2
    del dd_select
    del dd_filtered
    merged.drop('d_date_sk', axis=1, inplace=True)

    # now partition with cs_ship_addr_sk
    storage = output_address + "/part_" + str(key['task_id']) + "_"

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    # print(merged.dtypes)
    res = write_partitions(merged, ['cs_ship_addr_sk'], 'uniform', parall_2, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    info['outputs_info'] = outputs_info
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]
    return results


# In[22]:


# mkdir_if_not_exist(output_address)
def stage4(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cs = read_table(key)

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    cs = cs[cs.ca_state == 'GA'][['ca_address_sk']]

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    res = write_partitions(cs, ['ca_address_sk'], 'uniform', parall_2, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    info['outputs_info'] = outputs_info
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]
    return results


# In[23]:


# mkdir_if_not_exist(output_address)
def stage5(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    ca = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])
    cc = read_table(key['call_center'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    merged = cs.merge(ca, left_on='cs_ship_addr_sk', right_on='ca_address_sk')
    merged.drop('cs_ship_addr_sk', axis=1, inplace=True)

    list_addr = ['Williamson County', 'Williamson County', 'Williamson County', 'Williamson County',
                 'Williamson County']
    cc_p = cc[cc.cc_county.isin(list_addr)][['cc_call_center_sk']]

    # print(cc['cc_country'])
    merged2 = merged.merge(cc_p, left_on='cs_call_center_sk', right_on='cc_call_center_sk')

    toshuffle = merged2[['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit']]

    storage = output_address + "/part_" + str(key['task_id']) + "_"

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    res = write_partitions(toshuffle, ['cs_order_number'], 'uniform', parall_3, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    info['outputs_info'] = outputs_info
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]
    return results


# In[24]:


# mkdir_if_not_exist(output_address)
def stage6(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    a1 = pd.unique(cs['cs_order_number']).size
    a2 = cs['cs_ext_ship_cost'].sum()
    a3 = cs['cs_net_profit'].sum()

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    results = {}
    info = {}
    info['outputs_info'] = ''
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]
    return results


results = []
if os.path.exists(filename):
    results = pickle.load(open(filename, "r"))

table = "catalog_sales"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage1 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x + 1, len(all_locs))] for x in range(0, len(all_locs), 1)]
for loc in chunks:
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage1"
    tasks_stage1.append({'key': key})

if '1' not in stage_info_load:
    results_stage = execute_stage(stage1, tasks_stage1)
    # results_stage = execute_stage(stage1, [tasks_stage1[0]])
    stage1_info = [a['info'] for a in results_stage['results']]
    # print(stage1_info)
    stage_info_load['1'] = stage1_info[0]
    # print("111")
    # print(stage_info_load['1'])
    # print("end111")
    results.append(results_stage)
    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))

# exit(1)
# end stage1

table = "catalog_returns"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage2 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x + 10, len(all_locs))] for x in xrange(0, len(all_locs), 10)]
for loc in chunks:
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage2"

    tasks_stage2.append(key)

if '2' not in stage_info_load:
    results_stage = execute_stage(stage2, tasks_stage2)
    # results_stage = execute_stage(stage2, [tasks_stage2[0]])
    # results_stage = execute_local_stage(stage2, [tasks_stage2[0]])
    stage2_info = [a['info'] for a in results_stage['results']]
    stage_info_load['2'] = stage2_info[0]
    # print(stage_info_load['2'])
    results.append(results_stage)
    # exit(1)
    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))

# end stage2

tasks_stage3 = []
task_id = 0
date_dim_loc = get_locations("date_dim")[0]
for i in range(parall_1):
    # print(info)
    info = stage_info_load['1']
    info2 = stage_info_load['2']
    key = {}
    key['task_id'] = task_id
    key['prefix'] = temp_address + "intermediate/stage1/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage1)

    key['prefix2'] = temp_address + "intermediate/stage2/part_"
    key['suffix2'] = "_" + str(task_id) + ".csv"
    key['names2'] = info2["outputs_info"][0]['names']
    key['dtypes2'] = info2["outputs_info"][0]['dtypes']
    key['number_splits2'] = len(tasks_stage2)

    table = {}
    table['names'] = get_name_for_table("date_dim")
    table['dtypes'] = get_dtypes_for_table("date_dim")
    table['loc'] = date_dim_loc
    key['date_dim'] = table

    key['output_address'] = temp_address + "intermediate/stage3"

    tasks_stage3.append(key)
    task_id += 1

if '3' not in stage_info_load:
    results_stage = execute_stage(stage3, tasks_stage3)
    # results_stage = execute_local_stage(stage3, [tasks_stage3[0]])
    stage3_info = [a['info'] for a in results_stage['results']]
    stage_info_load['3'] = stage3_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))

# end stage3

table = "customer_address"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage4 = []
task_id = 0
for loc in get_locations(table):
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage4"

    tasks_stage4.append(key)

if '4' not in stage_info_load:
    results_stage = execute_stage(stage4, tasks_stage4)
    # results_stage = execute_local_stage(stage4, [tasks_stage4[0]])
    stage4_info = [a['info'] for a in results_stage['results']]
    stage_info_load['4'] = stage4_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))

# end stage4


tasks_stage5 = []
task_id = 0
call_center_loc = get_locations("call_center")[0]
# print(info["outputs_info"][0])
for i in range(parall_2):
    key = {}
    info = stage_info_load['3']
    info2 = stage_info_load['4']
    key = {}
    key['task_id'] = task_id

    key['prefix'] = temp_address + "intermediate/stage3/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage3)

    key['prefix2'] = temp_address + "intermediate/stage4/part_"
    key['suffix2'] = "_" + str(task_id) + ".csv"
    key['names2'] = info2["outputs_info"][0]['names']
    key['dtypes2'] = info2["outputs_info"][0]['dtypes']
    key['number_splits2'] = len(tasks_stage4)

    table = {}
    table['names'] = get_name_for_table("call_center")
    table['dtypes'] = get_dtypes_for_table("call_center")
    table['loc'] = call_center_loc
    key['call_center'] = table

    key['output_address'] = temp_address + "intermediate/stage5"

    tasks_stage5.append(key)
    task_id += 1

if '5' not in stage_info_load:
    results_stage = execute_stage(stage5, tasks_stage5)
    # results_stage = execute_local_stage(stage5, [tasks_stage5[0]])
    stage5_info = [a['info'] for a in results_stage['results']]
    stage_info_load['5'] = stage5_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))

# end stage5


tasks_stage6 = []
task_id = 0
for i in range(parall_3):
    key = {}
    info = stage_info_load['5']
    key = {}
    key['task_id'] = task_id

    key['prefix'] = temp_address + "intermediate/stage5/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage5)

    key['output_address'] = temp_address + "intermediate/stage6"

    tasks_stage6.append({'key':key})
    task_id += 1

if '6' not in stage_info_load:
    results_stage = execute_stage(stage6, tasks_stage6)
    stage6_info = [a['info'] for a in results_stage['results']]
    stage_info_load['6'] = stage6_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))
