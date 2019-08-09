# -*- coding: utf8 -*-
__author__ = 'sai'

import requests
import json
import itertools
import collections
import datetime
import urllib3
from pymongo import MongoClient
import time
urllib3.disable_warnings()


def get_block_hashs(_hashs, server, user_password):
    headers = {'content-type':'text/plain'}
    if isinstance(_hashs, collections.Iterable):
        hashs = _hashs
    else:
        hashs = [_hashs]
    payloads = []
    for h in hashs:
        payload = {
            "method": "getblockhash",
            "params": [h],
            "jsonrpc": "1.0",
            "id": "curltest"
                  }
        payloads.append(payload)
    session_request = requests.Session()
    session_request.auth = user_password
    session_request.headers.update(headers)
    for payload in payloads:
        response = session_request.post(server, data=json.dumps(payload))
        if response.ok:
            data = response.json()
            if 'result' in data:
                yield data['result']


def get_block_info(_hashs, server, user_password):
    headers = {'content-type':'text/plain'}
    if isinstance(_hashs, collections.Iterable) and not isinstance(_hashs, str):
        hashs = _hashs
    else:
        hashs = [_hashs]
    payloads = []
    for hash in hashs:
        payload = {
            "method": "getblock",
            "params": [hash],
            "jsonrpc": "1.0",
            "id": "curltest"
        }
        payloads.append(payload)
    session_request = requests.Session()
    session_request.auth = user_password
    session_request.headers.update(headers)
    for payload in payloads:
        response = session_request.post(server, data=json.dumps(payload))
        if response.ok:
            data = response.json()
            if 'result' in data:
                yield data['result']


def init_connect_to_mongodb(ip, port, dbname, username=None, password=None):
    """
    :param ip:  ip server MongoDB: mongodb.prod.int.nt-com.ru
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
            print(f"try {check_i}, connecting - error, sleep - 1 sec.")
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


def return_last_namecoinblock(server, user_password):
    headers = {'content-type':'text/plain'}
    payload = {
        "method": "getblockcount",
        "params": [],
        "jsonrpc": "1.0",
        "id": "curltest"
    }

    session_request = requests.Session()
    session_request.auth = user_password
    session_request.headers.update(headers)
    response = session_request.post(server, data=json.dumps(payload))
    if response.ok:
        try:
            data = response.json()
            return data['result']
        except Exception as e:
            print(str(e))
            print(f'errors:{response.text}')


def return_last_namecoinblock_local(col):
    try:
        last_block = col.find_one({'$query': {}, '$orderby': {'height': -1}})['height']
        return last_block
    except Exception as e:
        print(str(e))



if __name__ == '__main__':
    # mongodb
    ip_mongodb = "192.168.8.175"
    port_mongodb = "27017"
    mongo_user_password = ("", "")
    username_mongodb, password_mongodb = mongo_user_password

    dbname = "NamecoinExplorer"
    collection_name = "Blocks"

    cl_mongo = init_connect_to_mongodb(ip_mongodb, port_mongodb, dbname, username_mongodb, password_mongodb)
    db = cl_mongo[dbname]
    # --------
    server_rpc = "http://68.183.0.119:8336"
    server_rpc_user_password = ("user", "moscow")
    # ---------
    time_to_sleep = 360
    # ---------
    print('working with blocks...')
    while True:

        lastblock_from_chains = return_last_namecoinblock(server_rpc, server_rpc_user_password)
        lastblock_local = return_last_namecoinblock_local(db[collection_name])

        if lastblock_from_chains and lastblock_local:
            if lastblock_local < lastblock_from_chains:
                print(f'Need update local database.\nLast block local:{lastblock_local}\nLast block Namecoin:{lastblock_from_chains}')
                # ---------
                n_block = range(lastblock_local+1, lastblock_from_chains+1)
                count_in_block = 100
                group_blocks = grouper(count_in_block, n_block)
                i = 0
                for group_block in group_blocks:
                    i += 1
                    print("group of blocks:{}".format(i*count_in_block))
                    hashs = get_block_hashs(group_block, server_rpc, server_rpc_user_password)
                    data_block = get_block_info(hashs, server_rpc, server_rpc_user_password)
                    _tmp = []
                    for current_block in data_block:
                        if 'time' in current_block :
                            timeblock = datetime.datetime.utcfromtimestamp(current_block ['time'])
                            current_block['clean_datetime'] = timeblock
                        current_block['_id'] = current_block['hash']
                        _tmp.append(current_block )
                    print("inserted N blocks:{}".format(len(_tmp)))
                    try:
                        db[collection_name].insert_many(_tmp)
                    except Exception as e:
                        print(str(e))
            else:
                print("not need update...")
        print(f'time to sleep(sec.):{time_to_sleep}')
        time.sleep(time_to_sleep)




