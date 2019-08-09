# -*- coding: utf8 -*-
__author__ = 'sai'

import requests
import json
import itertools
import ipaddress
import re
import collections
import datetime
import concurrent.futures
import urllib3
from pymongo import MongoClient
import time
urllib3.disable_warnings()


def is_english(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


def init_connect_to_mongodb(ip, port, dbname, username=None, password=None):
    """
    :param ip:  ip server MongoDB
    :param port: 27017
    :return: True, if connected, and set value mongoclient - MongoClient
    """
    if username and password:
        connect_string_to = 'mongodb://{}:{}@{}:{}/{}'.format(username, password, ip, port, dbname)
    else:
        connect_string_to = 'mongodb://{}:{}/{}'.format(ip, port, dbname)

    check = False
    count_repeat = 4
    sleep_sec = 1
    check_i = 0

    while not check and check_i < count_repeat:
        try:
            client = MongoClient(connect_string_to, serverSelectionTimeoutMS=60)
            client.server_info()
            check = True
        except:
            print(f"try {check_i}, connecting - error, sleep - {sleep_sec} sec.")
            time.sleep(sleep_sec)
            check_i += 1
    if check:
        mongoclient = client
    return mongoclient


def grouper(count, iterable, fillvalue=None):
    """
    :param count: length of subblock
    :param iterable: array of data
    :param fillvalue: is fill value in last chain
    :return:
    grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    """
    args = [iter(iterable)] * count
    result = []
    for element in itertools.zip_longest(fillvalue=fillvalue, *args):
        tmp = filter(lambda y: y is not None, element)
        result.append(list(tmp))
    return result


def return_ip(text):
    result = []
    ipPattern = r'(?:[\d]{1,3})\.(?:[\d]{1,3})\.(?:[\d]{1,3})\.(?:[\d]{1,3})'
    ipaddress_subtmp = re.findall(ipPattern, text)
    if ipaddress_subtmp:
        for ip in ipaddress_subtmp:
            try:
                _ip = ipaddress.ip_address(ip.strip())
                if str(_ip) not in result:
                    result.append(str(_ip))
            except:
                pass
    if len(result) > 0:
        return result


def extract_email_version_v2(data):
    if '@' in data:
        emails = re.findall(r'[\w\*\w.-]+@[\w\.-]+', data)
        if len(emails) > 0:
            tmp = list(filter(lambda y: '.' in y and len(y) > 4, map(lambda y: y.strip().strip('.'), emails)))
            if len(tmp) > 0:
                result = list(map(lambda y: y.lower().rstrip('-'), filter(is_english, tmp)))
                if len(result) > 0:
                    for email in result:
                        if '.' in ''.join(email.split('@')[1:]):
                            yield email


def return_info_about_txs(_txs, server, user_password):
    headers = {'content-type': 'text/plain'}
    if isinstance(_txs, collections.Iterable) and not isinstance(_txs, str):
        txs = _txs
    else:
        txs = [_txs]
    payloads = []

    for tx in txs:
        headers = {'content-type':'text/plain'}
        payload = {
            "method": "getrawtransaction",
            "params": [tx, True],
            "jsonrpc": "1.0",
            "id": "curltest"
        }
        payloads.append(payload)
    session_request = requests.Session()
    session_request.auth = user_password
    session_request.headers.update(headers)

    for payload in payloads:
        response = session_request.post(
            server, data=json.dumps(payload))
        if response.ok:
            data = response.json()
            if 'result' in data:
                yield data['result']


def save_block(group_block_and_settings):

    def return_txs_from_mongodb(col, block_h):
        result = []
        rows = col.find({'height': {'$in': block_h}})
        for row in rows:
            result.extend(row['tx'])
        return result

    group_block, settings = group_block_and_settings

    ip, port, dbname, collection_name_blocks, collection_name_tx, \
    server_rpc, server_rpc_user_password, mongo_user_password = settings

    user_mongodb, pass_mongodb = mongo_user_password
    cl_mongo = init_connect_to_mongodb(ip, port, dbname, user_mongodb, pass_mongodb)
    db = cl_mongo[dbname]

    txs = return_txs_from_mongodb(db[collection_name_blocks], group_block)
    _tmp = []
    transactions = return_info_about_txs(txs, server_rpc, server_rpc_user_password)
    for _row in transactions:
        row = _row.copy()
        try:
            _emails = list(extract_email_version_v2(json.dumps(row)))
            if len(_emails) > 0:
                row['emails'] = _emails
        except:
            pass

        _ips = return_ip(json.dumps(row))
        if _ips:
            row['ips'] = _ips

        if 'time' in row :
            timetx = datetime.datetime.utcfromtimestamp(row ['time'])
            row['clean_datetime_tx'] = timetx
        if 'blocktime' in row:
            timeblock = datetime.datetime.utcfromtimestamp(row['blocktime'])
            row['clean_datetime_block'] = timeblock
        if 'vout' in row:
            for el in row['vout']:
                z = el.get('scriptPubKey')
                if 'nameOp' in z:
                    try:
                        row['clean_name'] = z['nameOp']['name']
                        row['clean_value'] = z['nameOp']['value']
                        row['clean_op'] = z['nameOp']['op']
                    except:
                        pass
        _tmp.append(row)
    if len(_tmp) > 0:
        db[collection_name_tx].insert_many(_tmp)
        time.sleep(0.3)
        return True


def pool_for_mongodb(blocks, struct_for_save_block, max_workers=16):
    count_block = len([b for block in blocks for b in block])
    if len(blocks) > 0:
        print('need update Tx, blocks:{}'.format(count_block))
        # not an elegant method, but also ...
        iters = [(block, struct_for_save_block) for block in blocks]
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            for block, result in zip(blocks, executor.map(save_block, iters)):
                if result:
                    print(f"blocks:{block[0]}-{block[-1]}, status:{result}")
                else:
                    print("errors:", block, result)


def return_need_blocks(ip, port, dbname, collection_name_blocks, mongo_user_password):

    def return_last_namecoinblock_local(col):
        try:
            last_block = col.find_one({'$query': {}, '$orderby': {'height': -1}})['height']
            return last_block
        except Exception as e:
            print(str(e))

    user, password = mongo_user_password
    cl_mongo = init_connect_to_mongodb(ip, port, dbname, user, password)
    db = cl_mongo[dbname]
    last_tx_hash = db[collection_name_tx].find_one({'$query': {}, '$orderby': {'clean_datetime_block': -1}})['blockhash']
    number_block_latest_tx = db[collection_name_blocks].find_one({'_id':last_tx_hash})['height']
    last_block_local = return_last_namecoinblock_local(db[collection_name_blocks])
    print(number_block_latest_tx, last_block_local)
    if number_block_latest_tx < last_block_local:
        n_block = range(number_block_latest_tx+1, last_block_local+1)
        count_in_block = 40
        group_blocks = grouper(count_in_block, n_block)
        return group_blocks


if __name__ == '__main__':
    ip_mongodb = "192.168.8.175"
    port_mongodb = "27017"
    mongo_user_password = ("", "")
    dbname = "NamecoinExplorer"
    collection_name_blocks = "Blocks"
    collection_name_tx = "Tx"
    # ----------
    server_rpc = "http://68.183.0.119:8336"
    server_rpc_user_password = ("user", "moscow")
    # ----------
    time_to_sleep = 360  # time to sleep in while

    struct_for_save_block = (ip_mongodb, port_mongodb, dbname, collection_name_blocks,
                             collection_name_tx, server_rpc, server_rpc_user_password, mongo_user_password)
    while True:
        # return struct with groups of blocks [[]...[]]
        group_blocks = return_need_blocks(ip_mongodb, port_mongodb, dbname, collection_name_blocks, mongo_user_password)
        if group_blocks:
            if len([b for block in group_blocks for b in block]) > 0:
                pool_for_mongodb(group_blocks, struct_for_save_block)
        else:
            print("skip operation")
        print(f'time to sleep(sec.):{time_to_sleep}')
        time.sleep(time_to_sleep)
