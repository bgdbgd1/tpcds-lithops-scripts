# coding: utf-8
from common import *
import pickle
import pandas as pd
import os

import time


print("Scale is " + str(scale))

if storage_mode == 'local':
    temp_address = "/Users/qifan/data/q" + query_name + "-temp/"
else:
    temp_address = "scale" + str(scale) + "/q" + query_name + "-temp/"


# implementing all stages

def stage1(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()
    output_address = key['output_address']
    cs = read_table(key)
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()
    wanted_columns = ['ws_order_number',
                      'ws_ext_ship_cost',
                      'ws_net_profit',
                      'ws_ship_date_sk',
                      'ws_ship_addr_sk',
                      'ws_web_site_sk',
                      'ws_warehouse_sk']
    cs_s = cs[wanted_columns]

    t1 = time.time()
    tc += t1 - t0

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cs_s, ['ws_order_number'], 'uniform', parall_1, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    # results['info'] = {}
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]

    return results


# In[20]:


# mkdir_if_not_exist(output_address)

def stage2(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cr = read_table(key)

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cr, ['wr_order_number'], 'uniform', parall_1, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    # results['info'] = {}
    results['breakdown'] = [tr, tc, tw, (tc + tc + tw)]
    return results


# In[21]:


# mkdir_if_not_exist(output_address)
# @profile
def stage3(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    # print(key['names'])
    # print(key['dtypes'])
    print("===BEFORE FIRST MULTIPLE SPLITS====")
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    print("===BEFORE SECOND MULTIPLE SPLITS====")
    cr = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])
    print("===AFTER MULTIPLE SPLITS====")

    d = read_table(key['date_dim'])
    print("===AFTER READ TABLE DATE DIM====")

    # return 1
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    cs_succient = cs[['ws_order_number', 'ws_warehouse_sk']]
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
    wh_uc = cs_succient.groupby(['ws_order_number']).agg({'ws_warehouse_sk': 'nunique'})
    target_order_numbers = wh_uc.loc[wh_uc['ws_warehouse_sk'] > 1].index.values
    cs_sj_f1 = cs.loc[cs['ws_order_number'].isin(target_order_numbers)]

    cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(cr.wr_order_number)]
    del cs_sj_f1
    # cs_sj_f2.rename(columns = {'cs_warehouse_sk_y':'cs_warehouse_sk'}, inplace = True)

    # join date_dim
    dd = d[['d_date', 'd_date_sk']]
    dd_select = dd[(pd.to_datetime(dd['d_date']) > pd.to_datetime('1999-02-01')) & (
            pd.to_datetime(dd['d_date']) < pd.to_datetime('1999-04-01'))]
    dd_filtered = dd_select[['d_date_sk']]

    merged = cs_sj_f2.merge(dd_filtered, left_on='ws_ship_date_sk', right_on='d_date_sk')
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
    print("=================BEFORE WRITE PARTITIONS==================")
    res = write_partitions(merged, ['ws_ship_addr_sk'], 'uniform', parall_2, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    # results['info'] = {}
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
    cs = cs[cs.ca_state == 'IL'][['ca_address_sk']]

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
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    # results['info'] = {}
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
    cc = read_table(key['web_site'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    merged = cs.merge(ca, left_on='ws_ship_addr_sk', right_on='ca_address_sk')
    merged.drop('ws_ship_addr_sk', axis=1, inplace=True)

    # list_addr = ['Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County']
    cc_p = cc[cc['web_company_name'] == 'pri'][['web_site_sk']]

    # print(cc['cc_country'])
    merged2 = merged.merge(cc_p, left_on='ws_web_site_sk', right_on='web_site_sk')

    toshuffle = merged2[['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit']]

    storage = output_address + "/part_" + str(key['task_id']) + "_"

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    res = write_partitions(toshuffle, ['ws_order_number'], 'uniform', parall_3, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    # results['info'] = {}
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

    a1 = pd.unique(cs['ws_order_number']).size
    a2 = cs['ws_ext_ship_cost'].sum()
    a3 = cs['ws_net_profit'].sum()

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
    results = pickle.load(open(filename, "rb"))

table = "web_sales"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage1 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x + 2, len(all_locs))] for x in range(0, len(all_locs), 2)]
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

table = "web_returns"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage2 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x + 10, len(all_locs))] for x in range(0, len(all_locs), 10)]
for loc in chunks:
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage2"

    tasks_stage2.append({'key': key})

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

    tasks_stage3.append({'key': key})
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

    tasks_stage4.append({'key': key})

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
call_center_loc = get_locations("web_site")[0]
# print(info["outputs_info"][0])
for i in range(parall_2):
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
    table['names'] = get_name_for_table("web_site")
    table['dtypes'] = get_dtypes_for_table("web_site")
    table['loc'] = call_center_loc
    key['web_site'] = table

    key['output_address'] = temp_address + "intermediate/stage5"

    tasks_stage5.append({'key': key})
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
    info = stage_info_load['5']
    key = {}
    key['task_id'] = task_id

    key['prefix'] = temp_address + "intermediate/stage5/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage5)

    key['output_address'] = temp_address + "intermediate/stage6"

    tasks_stage6.append({'key': key})
    task_id += 1

if '6' not in stage_info_load:
    results_stage = execute_stage(stage6, tasks_stage6)
    # results_stage = execute_local_stage(stage6, [tasks_stage6[0]])
    stage6_info = [a['info'] for a in results_stage['results']]
    stage_info_load['6'] = stage6_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))
