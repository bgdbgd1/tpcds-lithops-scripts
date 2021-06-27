# coding: utf-8
from common import *
import time
# SELECT
#   count(DISTINCT ws_order_number) AS `order count `,
#   sum(ws_ext_ship_cost) AS `total shipping cost `,
#   sum(ws_net_profit) AS `total net profit `
# FROM
#   web_sales ws1, date_dim, customer_address, web_site
# WHERE
#   d_date BETWEEN '1999-02-01' AND
#   (CAST('1999-02-01' AS DATE) + INTERVAL 60 days)
#     AND ws1.ws_ship_date_sk = d_date_sk
#     AND ws1.ws_ship_addr_sk = ca_address_sk
#     AND ca_state = 'IL'
#     AND ws1.ws_web_site_sk = web_site_sk
#     AND web_company_name = 'pri'
#     AND EXISTS(SELECT *
#                FROM web_sales ws2
#                WHERE ws1.ws_order_number = ws2.ws_order_number
#                  AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
#     AND NOT EXISTS(SELECT *
#                    FROM web_returns wr1
#                    WHERE ws1.ws_order_number = wr1.wr_order_number)
# ORDER BY count(DISTINCT ws_order_number)
# LIMIT 100

# WITH customer_total_return AS
# ( SELECT
#     sr_customer_sk AS ctr_customer_sk,
#     sr_store_sk AS ctr_store_sk,
#     sum(sr_return_amt) AS ctr_total_return
#   FROM store_returns, date_dim
#   WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
#   GROUP BY sr_customer_sk, sr_store_sk)
# SELECT c_customer_id
# FROM customer_total_return ctr1, store, customer
# WHERE ctr1.ctr_total_return >
#   (SELECT avg(ctr_total_return) * 1.2
#   FROM customer_total_return ctr2
#   WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
#   AND s_store_sk = ctr1.ctr_store_sk
#   AND s_state = 'TN'
#   AND ctr1.ctr_customer_sk = c_customer_sk
# ORDER BY c_customer_id
# LIMIT 100

# implementing all stages

def stage1(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    dd = read_table(key)

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    cr = dd[dd['d_year'] == 2000][['d_date_sk']]
    res = write_partitions(cr, ['d_date_sk'], 'uniform', parall_1, storage)
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


table = "date_dim"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage1 = []
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
    key['output_address'] = temp_address + "intermediate/stage1"

    tasks_stage1.append({'key': key})

######
if '1' not in stage_info_load:
    results_stage = execute_stage(stage1, tasks_stage1)
    stage1_info = [a['info'] for a in results_stage['results']]
    stage_info_load['1'] = stage1_info[0]
    results.append(results_stage)
    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))


def stage2(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cr = read_table(key)

    wanted_columns = ['sr_customer_sk',
                      'sr_store_sk',
                      'sr_return_amt',
                      'sr_returned_date_sk']

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cr, ['sr_returned_date_sk'], 'uniform', parall_1, storage)
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


table = "store_returns"
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

#######
#######
#######


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


def stage3(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    d = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    sr = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    merged = d.merge(sr, left_on='d_date_sk', right_on='sr_returned_date_sk')
    merged.drop('d_date_sk', axis=1, inplace=True)

    # print(cc['cc_country'])

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(merged, ['sr_customer_sk', 'sr_store_sk'], 'uniform', parall_2, storage)
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


tasks_stage3 = []
task_id = 0
for i in range(parall_1):
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

    key['output_address'] = temp_address + "intermediate/stage3"

    tasks_stage3.append({'key': key})
    task_id += 1

if '3' not in stage_info_load:
    results_stage = execute_stage(stage3, tasks_stage3)
    # results_stage = execute_stage(stage3, [tasks_stage3[0]])
    stage3_info = [a['info'] for a in results_stage['results']]
    stage_info_load['3'] = stage3_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))


def stage4(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    d = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    r = d.groupby(['sr_customer_sk', 'sr_store_sk']).agg({'sr_return_amt': 'sum'}).reset_index()
    r.rename(columns={'sr_store_sk': 'ctr_store_sk',
                      'sr_customer_sk': 'ctr_customer_sk',
                      'sr_return_amt': 'ctr_total_return'
                      }, inplace=True)

    # print(r)
    # print(cc['cc_country'])

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(r, ['ctr_store_sk'], 'uniform', parall_3, storage)
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


tasks_stage4 = []
task_id = 0
for i in range(parall_1):
    info = stage_info_load['3']
    key = {}
    key['task_id'] = task_id

    key['prefix'] = temp_address + "intermediate/stage3/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage3)

    key['output_address'] = temp_address + "intermediate/stage4"

    tasks_stage4.append({'key': key})
    task_id += 1

if '4' not in stage_info_load:
    results_stage = execute_stage(stage4, tasks_stage4)
    # results_stage = execute_local_stage(stage4, [tasks_stage4[0]])
    stage4_info = [a['info'] for a in results_stage['results']]
    stage_info_load['4'] = stage4_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))


def stage5(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    dd = read_table(key)[['c_customer_sk', 'c_customer_id']]

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(dd, ['c_customer_sk'], 'uniform', parall_3, storage)
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


table = "customer"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage5 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x + 10, len(all_locs))] for x in range(0, len(all_locs), 10)]
for loc in chunks:
    key = {}
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage5"

    tasks_stage5.append({'key': key})

if '5' not in stage_info_load:
    results_stage = execute_stage(stage5, tasks_stage5)
    # results_stage = execute_local_stage(stage5, [tasks_stage5[0]])
    stage5_info = [a['info'] for a in results_stage['results']]
    stage_info_load['5'] = stage5_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))


def stage6(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    d = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    sr = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    merged = d.merge(sr, left_on='ctr_customer_sk', right_on='c_customer_sk')
    merged_u = merged[['c_customer_id', 'ctr_store_sk', 'ctr_customer_sk', 'ctr_total_return']]

    # print(cc['cc_country'])

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(merged_u, ['ctr_store_sk'], 'uniform', parall_4, storage)
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


tasks_stage6 = []
task_id = 0
for i in range(parall_1):
    info = stage_info_load['4']
    info2 = stage_info_load['5']
    key = {}
    key['task_id'] = task_id

    key['prefix'] = temp_address + "intermediate/stage4/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage4)

    key['prefix2'] = temp_address + "intermediate/stage5/part_"
    key['suffix2'] = "_" + str(task_id) + ".csv"
    key['names2'] = info2["outputs_info"][0]['names']
    key['dtypes2'] = info2["outputs_info"][0]['dtypes']
    key['number_splits2'] = len(tasks_stage5)

    key['output_address'] = temp_address + "intermediate/stage6"

    tasks_stage6.append({'key': key})
    task_id += 1

if '6' not in stage_info_load:
    results_stage = execute_stage(stage6, tasks_stage6)
    # results_stage = execute_local_stage(stage6, [tasks_stage6[0]])
    stage6_info = [a['info'] for a in results_stage['results'] if len(a['info']['outputs_info']) > 0]
    stage_info_load['6'] = stage6_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))


def stage7(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    dd = read_table(key)[['s_state', 's_store_sk']]

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    filtered = dd[dd['s_state'] == 'TN']
    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(filtered, ['s_store_sk'], 'uniform', parall_4, storage)
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


table = "store"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage7 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x + 1, len(all_locs))] for x in range(0, len(all_locs), 1)]
for loc in chunks:
    key = {}
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage7"

    tasks_stage7.append({'key': key})

if '7' not in stage_info_load:
    results_stage = execute_stage(stage7, tasks_stage7)
    # results_stage = execute_local_stage(stage7, [tasks_stage7[0]])
    stage7_info = [a['info'] for a in results_stage['results']]
    stage_info_load['7'] = stage7_info[0]
    results.append(results_stage)

    pickle.dump(results, open(filename, 'wb'))
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))


def stage8(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    d = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    sr = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    merged = d.merge(sr, left_on='ctr_store_sk', right_on='s_store_sk')
    merged_u = merged[['ctr_store_sk', 'c_customer_id', 'ctr_total_return', 'ctr_customer_sk']]

    r_avg = merged_u.groupby(['ctr_store_sk']).agg({'ctr_total_return': 'mean'}).reset_index()
    r_avg.rename(columns={'ctr_total_return': 'ctr_avg'}, inplace=True)

    # print(r_avg)
    # print("aaa")
    # aaa = [d.empty, sr.empty, merged_u.empty, r_avg.empty]
    # print(",".join([str(a) for a in aaa]))
    merged_u2 = merged_u.merge(r_avg, left_on='ctr_store_sk', right_on='ctr_store_sk')
    final_merge = merged_u2[merged_u2['ctr_total_return'] > merged_u2['ctr_avg']]
    final = final_merge[['c_customer_id']].drop_duplicates().sort_values(by=['c_customer_id'])
    # print(final)
    # print(cc['cc_country'])

    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(merged_u, ['c_customer_id'], 'uniform', parall_5, storage)
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


tasks_stage8 = []
task_id = 0
for i in range(parall_4):
    info = stage_info_load['6']
    info2 = stage_info_load['7']
    key = {}
    key['task_id'] = task_id

    key['prefix'] = temp_address + "intermediate/stage6/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage6)

    key['prefix2'] = temp_address + "intermediate/stage7/part_"
    key['suffix2'] = "_" + str(task_id) + ".csv"
    key['names2'] = info2["outputs_info"][0]['names']
    key['dtypes2'] = info2["outputs_info"][0]['dtypes']
    key['number_splits2'] = len(tasks_stage7)

    key['output_address'] = temp_address + "intermediate/stage8"

    tasks_stage8.append({'key': key})
    task_id += 1

# results_stage = execute_local_stage(stage8, tasks_stage8[7:8])
# exit(0)

if '8' not in stage_info_load:
    results_stage = execute_stage(stage8, tasks_stage8)

    stage8_info = [a['info'] for a in results_stage['results']]
    stage_info_load['8'] = stage8_info[0]
    results.append(results_stage)
    pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))
    pickle.dump(results, open(filename, 'wb'))
