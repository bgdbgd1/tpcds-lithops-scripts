import pickle
import pandas as pd
import os
import numpy as np
import datetime
import table_schemas
from IPython import get_ipython
from config_vars import *

from s3fs import S3FileSystem

import redis

import time
from hashlib import md5

import boto3
from io import BytesIO, StringIO

def get_type(typename):
    if typename == "date":
        return datetime.datetime
    if "decimal" in typename:
        return np.dtype("float")
    if typename == "int" or typename == "long" or typename == "integer" or typename == "identifier":
        return np.dtype("float")
    if typename == "float":
        return np.dtype(typename)
    if typename == "string" or typename == "varchar" or typename == "char":
        return np.dtype(np.unicode)
    raise Exception("Not supported type: " + typename)


def get_s3_locations(table):
    print("WARNING: get from S3 locations, might be slow locally.")
    s3 = S3FileSystem()
    ls_path = os.path.join(S3_BUCKET, "tpcds-data", "scale" + str(scale), table)
    all_files = s3.ls(ls_path)
    return ["s3://" + f for f in all_files if f.endswith(".csv")]


def get_local_locations(table):
    print("WARNING: get from local locations, might not work on lamdbda.")
    files = []
    path = "/Users/qifan/data/tpcds-scale10/" + table
    for f in os.listdir(path):
        if f.endswith(".csv"):
            files.append(os.path.join(path, f))
    return files


def get_name_for_table(tablename):
    schema = table_schemas.schemas[tablename]
    names = [a[0] for a in schema]
    return names


def get_dtypes_for_table(tablename):
    schema = table_schemas.schemas[tablename]
    dtypes = {}
    for a, b in schema:
        dtypes[a] = get_type(b)
    return dtypes


def read_local_table(key):
    loc = key['loc']
    names = list(key['names'])
    names.append("")
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype(np.unicode)
    part_data = pd.read_table(loc,
                              delimiter="|",
                              header=None,
                              names=names,
                              usecols=range(len(names) - 1),
                              dtype=dtypes,
                              na_values="-",
                              parse_dates=parse_dates)
    # print(part_data.info())
    return part_data


def read_s3_table(key, s3_client=None):
    loc = key['loc']
    names = list(key['names'])
    names.append("")
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype(np.unicode)
    if s3_client == None:
        s3_client = boto3.client("s3")
    data = []
    if isinstance(key['loc'], str):
        loc = key['loc']
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=loc[33:])['Body'].read()
        data.append(obj)
    else:
        for loc in key['loc']:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=loc[33:])['Body'].read()
            data.append(obj)

    data_str = []
    for obj in data:
        data_str.append(obj.decode('utf-8'))
    joined_data = "".join(data_str)
    joined = StringIO(joined_data)
    part_data = pd.read_table(joined,
                              delimiter="|",
                              header=None,
                              names=names,
                              usecols=range(len(names) - 1),
                              dtype=dtypes,
                              na_values="-",
                              parse_dates=parse_dates)
    # print(part_data.info())
    return part_data


def hash_key_to_index(key, number):
    return int(md5(key).hexdigest()[8:], 16) % number


def my_hash_function(row, indices):
    return hash("".join([str(row[index]) for index in indices])) % 65536


def add_bin(df, indices, bintype, partitions):
    hvalues = df.apply(lambda x: my_hash_function(tuple(x), indices), axis=1)
    if bintype == 'uniform':
        # _, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
        bins = np.linspace(0, 65536, num=(partitions + 1), endpoint=True)
    elif bintype == 'sample':
        samples = hvalues.sample(n=min(hvalues.size, max(hvalues.size / 8, 65536)))
        _, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
    else:
        raise Exception()
    # print("here is " + str(time.time() - tstart))
    if hvalues.empty:
        return []

    df['bin'] = pd.cut(hvalues, bins=bins, labels=False, include_lowest=False)
    # print("here is " + str(time.time() - tstart))
    return bins


def write_local_intermediate(table, output_loc):
    csv_buffer = BytesIO()
    output_info = {}
    if 'bin' in table.columns:
        slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    else:
        slt_columns = table.columns

    table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)
    output_info['loc'] = output_loc
    output_info['names'] = slt_columns
    output_info['dtypes'] = table.dtypes[slt_columns]

    return output_info


def write_s3_intermediate(output_loc, table, s3_client=None):
    csv_buffer = BytesIO()
    if 'bin' in table.columns:
        slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    else:
        slt_columns = table.columns
    table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)
    if s3_client == None:
        s3_client = boto3.client('s3')

    bucket_index = int(md5(output_loc.encode()).hexdigest()[8:], 16) % N_BUCKETS
    s3_client.put_object(Bucket=S3_BUCKET,
                         Key=f'{str(bucket_index)}/{output_loc}',
                         Body=csv_buffer.getvalue())

    output_info = {}
    output_info['loc'] = output_loc
    output_info['names'] = slt_columns
    output_info['dtypes'] = table.dtypes[slt_columns]

    return output_info


def write_redis_intermediate(output_loc, table, redis_client=None):
    csv_buffer = BytesIO()
    if 'bin' in table.columns:
        slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    else:
        slt_columns = table.columns

    table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)
    if redis_client == None:
        redis_index = hash_key_to_index(output_loc, len(HOSTNAMES))
        redis_client = redis.StrictRedis(host=HOSTNAMES[redis_index], port=6379, db=0)
        # redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    redis_client.set(output_loc, csv_buffer.getvalue())

    output_info = {}
    output_info['loc'] = output_loc
    output_info['names'] = slt_columns
    output_info['dtypes'] = table.dtypes[slt_columns]

    return output_info


def write_local_partitions(df, column_names, bintype, partitions, storage):
    # print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    # print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    # print((bins))
    # print(df)
    t1 = time.time()
    outputs_info = []
    for bin_index in range(len(bins)):
        split = df[df['bin'] == bin_index]
        if split.size > 0:
            output_info = {}
            split.drop('bin', axis=1, inplace=True)
            # print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            outputs_info.append(write_local_intermediate(split, output_loc))
            # print(split.size)
    t2 = time.time()

    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1 - t0), (t2 - t1)]
    return results


def write_s3_partitions(df, column_names, bintype, partitions, storage):
    # print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    # print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
    # print("t1 - t0 is " + str(t1-t0))
    # print((bins))
    # print(df)
    s3_client = boto3.client("s3")
    outputs_info = []

    def write_task(bin_index):
        split = df[df['bin'] == bin_index]
        if split.size > 0 or split.size < 1:
            # print(split.size)
            split.drop('bin', axis=1, inplace=True)
            # print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            outputs_info.append(write_s3_intermediate(output_loc, split, s3_client))
    print("===================== BINS ==================")
    print(bins)
    for i in range(len(bins)):
        write_task(i)
    # write_pool = ThreadPool(1)
    # write_pool.map(write_task, range(len(bins)))
    # write_pool.close()
    # write_pool.join()
    t2 = time.time()

    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1 - t0), (t2 - t1)]
    return results


def write_redis_partitions(df, column_names, bintype, partitions, storage):
    # print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    # print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
    # print("t1 - t0 is " + str(t1-t0))
    # print((bins))
    # print(df)
    redis_clients = []
    pipes = []
    for hostname in HOSTNAMES:
        # redis_client = redis.StrictRedis(host=hostname, port=6379, db=0)
        redis_client = redis.Redis(host=hostname, port=6379, db=0)
        redis_clients.append(redis_client)
        pipes.append(redis_client.pipeline())
    # redis_client = redis.StrictRedis(host=redis_hostname, port=6379, db=0)

    # redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)

    outputs_info = []

    def write_task(bin_index):
        split = df[df['bin'] == bin_index]
        if split.size > 0 or split.size < 1:
            # print(split.size)
            # split.drop('bin', axis=1, inplace=True)
            # print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            redis_index = hash_key_to_index(output_loc, len(HOSTNAMES))
            # redis_client = redis_clients[redis_index]
            redis_client = pipes[redis_index]
            outputs_info.append(write_redis_intermediate(output_loc, split, redis_client))

    # write_pool = ThreadPool(1)
    # write_pool.map(write_task, range(len(bins)))
    # write_pool.close()
    # write_pool.join()
    for i in range(len(bins)):
       write_task(i)
    t2 = time.time()

    for pipe in pipes:
        pipe.execute()
    for redis_client in redis_clients:
        redis_client.connection_pool.disconnect()

    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1 - t0), (t2 - t1)]
    return results


def read_local_intermediate(key):
    names = list(key['names'])
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype(np.unicode)
    part_data = pd.read_table(key['loc'],
                              delimiter="|",
                              header=None,
                              names=names,
                              dtype=dtypes,
                              parse_dates=parse_dates)
    return part_data


def read_s3_intermediate(key, s3_client=None):
    bucket_index = int(md5(key['loc'].encode()).hexdigest()[8:], 16) % N_BUCKETS

    names = list(key['names'])
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype(np.unicode)
    if s3_client == None:
        s3_client = boto3.client("s3")
    # print('qifan-tpcds-' + str(bucket_index))
    # print(key['loc'])
    obj = s3_client.get_object(Bucket=S3_BUCKET,
                               Key=f'{str(bucket_index)}/{key["loc"]}')
    # obj = s3_client.get_object(Bucket='qifan-tpcds-' + str(bucket_index), Key=key['loc'])
    # print(key['loc'] + "")
    part_data = pd.read_table(BytesIO(obj['Body'].read()),
                              delimiter="|",
                              header=None,
                              names=names,
                              dtype=dtypes,
                              parse_dates=parse_dates)
    # print(part_data.info())
    return part_data


def read_redis_intermediate(key, redis_client=None):
    # bucket_index = int(md5(key['loc']).hexdigest()[8:], 16) % n_buckets

    names = list(key['names'])
    dtypes_raw = key['dtypes']
    if isinstance(dtypes_raw, dict):
        dtypes = dtypes_raw
    else:
        dtypes = {}
        for i in range(len(names)):
            dtypes[names[i]] = dtypes_raw[i]

    # print(dtypes)
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype(np.unicode)
    if redis_client == None:
        redis_index = hash_key_to_index(key['loc'], len(HOSTNAMES))
        redis_client = redis.StrictRedis(host=HOSTNAMES[redis_index], port=6379, db=0)

    part_data = pd.read_table(BytesIO(redis_client.get(key['loc'])),
                              delimiter="|",
                              header=None,
                              names=names,
                              dtype=dtypes,
                              parse_dates=parse_dates)
    # print(part_data.info())
    return part_data


def convert_buffer_to_table(names, dtypes, data):
    # bucket_index = int(md5(key['loc']).hexdigest()[8:], 16) % n_buckets

    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype(np.unicode)

    part_data = pd.read_table(BytesIO(data).getbuffer(),
                              delimiter="|",
                              header=None,
                              names=names,
                              dtype=dtypes,
                              parse_dates=parse_dates)
    # print(part_data.info())
    return part_data


def mkdir_if_not_exist(path):
    if STORAGE_MODE == 'local':
        get_ipython().system(u'mkdir -p $path ')


def read_local_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    key = {}
    key['names'] = names
    dtypes_dict = {}
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]
    key['dtypes'] = dtypes_dict
    ds = []
    for i in range(number_splits):
        key['loc'] = prefix + str(i) + suffix
        d = read_local_intermediate(key)
        ds.append(d)
    return pd.concat(ds)


def read_s3_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    dtypes_dict = {}
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]

    ds = []
    s3_client = boto3.client("s3")

    def read_work(split_index):
        key = {}
        key['names'] = names
        key['dtypes'] = dtypes_dict
        key['loc'] = prefix + str(split_index) + suffix
        d = read_s3_intermediate(key, s3_client)
        ds.append(d)

    # read_pool = ThreadPool(1)
    # read_pool.map(read_work, range(number_splits))
    for split in range(number_splits):
        read_work(split)
    # read_pool.close()
    # read_pool.join()

    return pd.concat(ds)


def read_redis_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    dtypes_dict = {}
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]

    ds = []
    # redis_client = redis.StrictRedis(host=redis_hostname, port=6379, db=0)
    # redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    redis_clients = []
    pipes = []
    for hostname in HOSTNAMES:
        # redis_client = redis.StrictRedis(host=hostname, port=6379, db=0)
        redis_client = redis.Redis(host=hostname, port=6379, db=0)
        redis_clients.append(redis_client)
        pipes.append(redis_client.pipeline())

    def read_work(split_index):
        key = {}
        # key['names'] = names
        # key['dtypes'] = dtypes_dict
        key['loc'] = prefix + str(split_index) + suffix
        redis_index = hash_key_to_index(key['loc'], len(HOSTNAMES))
        # redis_client = redis_clients[redis_index]
        pipes[redis_index].get(key['loc'])
        # redis_client = pipes[redis_index]
        # d = read_redis_intermediate(key, redis_client)
        # ds.append(d)

    # read_pool = ThreadPool(64)
    # read_pool.map(read_work, range(number_splits))
    # read_pool.close()
    # read_pool.join()
    for i in range(number_splits):
        read_work(i)
    ps = time.time()
    read_data = []
    for pipe in pipes:
        current_data = pipe.execute()
        # print(len(current_data))
        read_data.extend(current_data)
    pe = time.time()
    # print("pipe time : " + str(pe-ps))
    for redis_client in redis_clients:
        redis_client.connection_pool.disconnect()

    # return pd.concat(ds)
    # if None in read_data:
    #    print("None in read_data")
    #    print("number of Nones: " + str(len([None for v in read_data if v is None])) + " " + str(len(read_data)))
    # print(read_data)
    read_data = [v for v in read_data if v is not None]
    return convert_buffer_to_table(names, dtypes_dict, "".join(read_data))


def read_table(key):
    if STORAGE_MODE == "local":
        return read_local_table(key)
    else:
        return read_s3_table(key)


def read_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    if STORAGE_MODE == "local":
        return read_local_multiple_splits(names, dtypes, prefix, number_splits, suffix)
    elif STORAGE_MODE == "s3-only":
        return read_s3_multiple_splits(names, dtypes, prefix, number_splits, suffix)
    else:
        return read_redis_multiple_splits(names, dtypes, prefix, number_splits, suffix)


def read_intermediate(key):
    if STORAGE_MODE == "local":
        return read_local_intermediate(key)
    elif STORAGE_MODE == "s3-only":
        return read_s3_intermediate(key)
    else:
        return read_redis_intermediate(key)


def write_intermediate(table, output_loc):
    res = None
    if STORAGE_MODE == "local":
        res = write_local_intermediate(output_loc, table)
    elif STORAGE_MODE == "s3-only":
        res = write_s3_intermediate(output_loc, table)
    else:
        res = write_redis_intermediate(output_loc, table)
    return pickle.dumps([res])


def write_partitions(df, column_names, bintype, partitions, storage):
    res = None
    if STORAGE_MODE == "local":
        res = write_local_partitions(df, column_names, bintype, partitions, storage)
    elif STORAGE_MODE == "s3-only":
        res = write_s3_partitions(df, column_names, bintype, partitions, storage)
    else:
        res = write_redis_partitions(df, column_names, bintype, partitions, storage)
    if 'outputs_info' in res and res['outputs_info'] != '':
        res['outputs_info'] = pickle.dumps(res['outputs_info'])
    return res


def get_locations(table):
    if STORAGE_MODE == "local":
        return get_local_locations(table)
    else:
        return get_s3_locations(table)


# def execute_lambda_stage(wrenexec, stage_function, tasks):
#     t0 = time.time()
#     # futures = wrenexec.map(stage_function, tasks)
#     # pywren.wait(futures, 1, 64, 1)
#     for task in tasks:
#         task['key']['write_output'] = True
#     futures = wrenexec.map(stage_function, tasks)
#     # futures = wrenexec.map_sync_with_rate_and_retries(stage_function, tasks, straggler=False, WAIT_DUR_SEC=5,
#     #                                                   rate=pywren_rate)
#     results = wrenexec.get_result(futures)
#     # results = [f.result() for f in futures]
#     # run_statuses = [f.run_status for f in futures]
#     # invoke_statuses = [f.invoke_status for f in futures]
#     t1 = time.time()
#     res = {'results': results,
#            't0': t0,
#            't1': t1}
#     return res
#
#
# def execute_local_stage(stage_function, tasks):
#     stage_info = []
#     count = 0
#     for task in tasks:
#         print(count)
#         count += 1
#         task['write_output'] = True
#         stage_info.append(stage_function(task))
#     res = {'results': stage_info}
#     return res
#
#
# def execute_stage(stage_function, tasks):
#     res = None
#     if execution_mode == 'local':
#         res = execute_local_stage(stage_function, tasks)
#     else:
#         res = execute_lambda_stage(stage_function, tasks)
#
#     for rr in res['results']:
#         if rr['info']['outputs_info'] != '':
#             rr['info']['outputs_info'] = pickle.loads(rr['info']['outputs_info'])
#     return res
