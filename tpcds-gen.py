import lithops
import boto3
import os
from config_vars import S3_BUCKET
from smart_open import open
from lithops import FunctionExecutor

from util import copyfileobj

if __name__ == "__main__":
    import logging
    import subprocess
    import gc
    import time


    def run_all_commands_local(tasks):
        results = []
        for task in tasks:
            res = run_command_local(task['key'])
            results.append(res)
        return results


    def run_command_local(key, upload_file=True):
        client = boto3.client('s3', 'eu-central-1')
        for i in range(0, 5):
            table = key['table']
            start_index = key['start_index']
            total = key['total']
            scale = key['scale']
            index = start_index + i
            if index > total:
                return "good"

            if total == 1:
                command = ["./dsdgen",
                           "-table", table,
                           "-scale", str(scale),
                           "-force",
                           "-suffix", ".csv",
                           "-distribution", "tpcds.idx",
                           ]
            else:
                command = ["./dsdgen",
                           "-table", table,
                           "-scale", str(scale),
                           "-force",
                           "-suffix", ".csv",
                           "-parallel", str(total),
                           "-child", str(index),
                           "-distribution", "tpcds.idx",
                           ]

            subprocess.run(command)
            if upload_file:
                filename = table + "_" + str(index) + "_" + str(total) + ".csv"
                fullpathfile = filename
                if total == 1:
                    srcfullpath = table + ".csv"
                    res = subprocess.run(["mv", srcfullpath, fullpathfile])
                if not os.path.isfile(fullpathfile):
                    print(f"bad: fullpathfile - {fullpathfile}, total - {total}")
                    return "bad"
                keyname = "scale" + str(scale) + "/" + table + "/" + filename
                put_start = time.time()
                client.upload_file(fullpathfile, S3_BUCKET, 'tpcds-data/' + keyname)
                put_end = time.time()
                # res = subprocess.check_output(["rm", fullpathfile])
                if "sales" in table:
                    return_table = table.split("_")[0] + "_returns"
                    return_filename = return_table + "_" + str(index) + "_" + str(total) + ".csv"
                    return_fullpathfile = return_filename
                    if total == 1:
                        src_return_fullpath = return_table + ".csv"
                        res = subprocess.check_output(["mv", src_return_fullpath, return_fullpathfile])
                    return_keyname = "scale" + str(scale) + "/" + return_table + "/" + return_filename
                    client.upload_file(return_fullpathfile, S3_BUCKET, 'tpcds-data/' + return_keyname)
                    # res = subprocess.check_output(["rm", return_fullpathfile])
                print(str(index) + " th object uploaded using " + str(put_end - put_start) + " seconds.")
        return "good"


    def run_command(key):
        logger = logging.getLogger(__name__)

        client = boto3.client('s3', 'eu-central-1')
        if os.path.isfile('dsdgen'):
            print("=====================HEREEEEE==========================")
        else:
            print("=====================NOT HEREEEEE==========================")
        # else:
        #     print("=================FILE dsdgen IS HERE=================")

        for i in range(0, 5):
            table = key['table']
            start_index = key['start_index']
            total = key['total']
            scale = key['scale']
            index = start_index + i
            if index > total:
                return "good"

            if total == 1:
                command = ["./dsdgen",
                           "-table", table,
                           "-scale", str(scale),
                           "-force",
                           "-suffix", ".csv",
                           "-distribution", "tpcds.idx",
                           ]
            else:
                command = ["./dsdgen",
                           "-table", table,
                           "-scale", str(scale),
                           "-force",
                           "-suffix", ".csv",
                           "-parallel", str(total),
                           "-child", str(index),
                           "-distribution", "tpcds.idx",
                           ]
            subprocess.run(command)

            # with open(f's3://{S3_BUCKET}/test_file.csv', 'wb') as dest_file:
            #     with subprocess.Popen(command, stdout=subprocess.PIPE) as p:
            #         with p.stdout as genoutput:
            #             copyfileobj(genoutput, dest_file)
            #         returncode = p.wait()
            #         if returncode != 0:
            #             raise Exception(f'Non-zero return code for gensort: {returncode}')
            # subprocess.run(command)
            res = subprocess.check_output(["ls"])
            print(res)

            filename = table + "_" + str(index) + "_" + str(total) + ".csv"
            fullpathfile = filename
            if total == 1:
                srcfullpath = table + ".csv"
                res = subprocess.check_output(["mv", srcfullpath, fullpathfile])
            if not os.path.isfile(fullpathfile):
                print(f"bad: fullpathfile - {fullpathfile}, total - {total}")
                return "bad"
            keyname = "scale" + str(scale) + "/" + table + "/" + filename
            put_start = time.time()
            client.upload_file(fullpathfile, S3_BUCKET, 'tpcds-data/' + keyname)
            put_end = time.time()
            res = subprocess.check_output(["rm", fullpathfile])
            if "sales" in table:
                return_table = table.split("_")[0] + "_returns"
                return_filename = return_table + "_" + str(index) + "_" + str(total) + ".csv"
                return_fullpathfile = return_filename
                if total == 1:
                    src_return_fullpath = return_table + ".csv"
                    res = subprocess.check_output(["mv", src_return_fullpath, return_fullpathfile])
                return_keyname = "scale" + str(scale) + "/" + return_table + "/" + return_filename
                client.upload_file(return_fullpathfile, S3_BUCKET, 'tpcds-data/' + return_keyname)
                res = subprocess.check_output(["rm", return_fullpathfile])
            logger.info(str(index) + " th object uploaded using " + str(put_end - put_start) + " seconds.")
        return "good"

    tables_1000 = [("call_center", 1),
                   ("catalog_page", 1),
                   ("catalog_sales", 3614),
                   ("customer", 18),
                   ("customer_address", 8),
                   ("customer_demographics", 1),
                   ("date_dim", 1),
                   ("household_demographics", 1),
                   ("income_band", 1),
                   ("inventory", 140),
                   ("item", 1),
                   ("promotion", 1),
                   ("reason", 1),
                   ("ship_mode", 1),
                   ("store", 1),
                   ("store_sales", 5248),
                   ("time_dim", 1),
                   ("warehouse", 1),
                   ("web_page", 1),
                   ("web_sales", 1808),
                   ("web_site", 1)]
    tables_100 = [("call_center", 1),
                  ("catalog_page", 1),
                  ("catalog_sales", 322),
                  ("customer", 3),
                  ("customer_address", 1),
                  ("customer_demographics", 1),
                  ("date_dim", 1),
                  ("household_demographics", 1),
                  ("income_band", 1),
                  ("inventory", 90),
                  ("item", 1),
                  ("promotion", 1),
                  ("reason", 1),
                  ("ship_mode", 1),
                  ("store", 1),
                  ("store_sales", 433),
                  ("time_dim", 1),
                  ("warehouse", 1),
                  ("web_page", 1),
                  ("web_sales", 166),
                  ("web_site", 1)]
    tables_10 = [("call_center", 1),
                 ("catalog_page", 1),
                 ("catalog_sales", 33),
                 ("customer", 1),
                 ("customer_address", 1),
                 ("customer_demographics", 1),
                 ("date_dim", 1),
                 ("household_demographics", 1),
                 ("income_band", 1),
                 ("inventory", 27),
                 ("item", 1),
                 ("promotion", 1),
                 ("reason", 1),
                 ("ship_mode", 1),
                 ("store", 1),
                 ("store_sales", 44),
                 ("time_dim", 1),
                 ("warehouse", 1),
                 ("web_page", 1),
                 ("web_sales", 109),
                 ("web_site", 1)]
    all_tables = {10: tables_10, 100: tables_100, 1000: tables_1000}

    scale = 10
    passed_tasks = []
    for (table, total) in all_tables[scale]:
        if table is not "web_sales":
            continue
        print(table + " " + str(total))

        for i in range(1, total + 1, 5):
            key = {}
            key['total'] = total
            key['scale'] = scale
            key['table'] = table
            key['start_index'] = i
            passed_tasks.append({'key': key})

    res = run_all_commands_local(passed_tasks)
    # with FunctionExecutor(runtime='bogdan/tpcds-scripts-linux-2') as fexec:
    #     fut = fexec.map(run_command, [passed_tasks[0]])
    #     res = fexec.get_result(fut)
    # pywren.wait(fut)
    # res = [f.result() for f in fut]
    print("good:" + str(res.count("good")) + " bad:" + str(res.count("bad")) + " total:" + str(len(res)))
    # exit(0)
