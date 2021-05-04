import readline
import shlex
import urllib.parse
import os
import json
import subprocess
import pandas as pd
import sys
from tabulate import tabulate
import requests
import time
import hashlib

from websocket import create_connection

port = '26657' #tm port
host = 'localhost' #tm host
dbhost = 'localhost' #db host
dbport = '5432' #db port

def search_txns(txnstrs):
    for q in txnstrs:
        retry = 50
        while True:
            h = hashlib.new("sha1", q.encode())

            qstr = "http://127.0.0.1:" + port + "/"

            query = "\"pgwrite.tx+%3D+'" + str(h.hexdigest()) + "'\""
            qstr += "tx_search?query=" + query + "&prove=true"

            qstr = qstr.replace("%E2%80%9C", "%5C%22") # weird encoding issues
            qstr = qstr.replace("%E2%80%9D", "%5C%22")

            #print(qstr)
            x = requests.get(qstr)
            xj = x.json()

            if int(xj["result"]["total_count"]) == 0 and retry > 0:
                time.sleep(.1)
                print("Retrying: " + q)
                retry = retry - 1
            else:
                break




def main(argv):
    print('USAGE: python batch_sql.py src n_nodes consistency fname')
    txnstrs = []

    SQLFILE = argv[3]
    CONSISTENCY = argv[2] # read consistency
    N_NODES = int(argv[1]) # number of nodes in network
    SRC = argv[0] # directory with source code

    if N_NODES < 0 or N_NODES > 9:
        print("Usage: python shell.py <src> <n_nodes> <consistency_level>")
        return

    ## Hacksy, but first discover all validators addresses
    ## hacky because we know these hosts/ports. Would have
    ## to discover through some other mechanism in a real
    ## network...
    val_map = {}
    for i in range(N_NODES):
        sink = SRC
        if i != 0:
            sink += str(i)

        with open(sink + "/tmp/config/priv_validator_key.json") as vfile:
            validator = json.load(vfile)
            addr = validator["address"]

        val_map[addr] = str(26657+100*i) # Hard Coded Addr.

    stmt = ""
    txn_flag = 0

    sqlfile = open(SQLFILE, "r")
    sql_stmt = sqlfile.readline()

    while sql_stmt:
        tmp_stmt = sql_stmt
        if tmp_stmt == "":
            stmt += " "
            continue

        stmt += tmp_stmt
        if tmp_stmt.find(";") == -1:
            continue

        if not txn_flag and len(stmt) > 5 and stmt[0:6] == 'BEGIN;':
            # 'begin' transaction block
            txn_flag = 1
        
        if len(stmt) > 3 and stmt[-4:] == "END;":
            # 'end' transaction block  
            stmt = stmt[0:(stmt.rfind(";"))] # get rid of rightmost ;

            raw_stmt = stmt

            # Convert to URI/HTTP - a REST like interface for the backend tendermint RPC
            qstr = "curl -s \'" + host + ":" + port + "/"
            stmt = stmt.replace("\n", " ")
            stmt = stmt.replace("\\", "\\\\")
            stmt = stmt.strip()

            # create txn
            tmp = {}
            tmp['tx'] = "\"" + stmt + "\""
            enc = urllib.parse.urlencode(tmp)
            


            qstr += "broadcast_tx_async?" + enc + "\'"

            qstr = qstr.replace("%E2%80%9C", "%5C%22") # weird encoding issues
            qstr = qstr.replace("%E2%80%9D", "%5C%22")

            #print(qstr)
            output = os.popen(qstr).read() 
            y = json.loads(str(output))
            if not 'error' in y:
                txnstrs.append(raw_stmt) # store txn string to lookup later

            # reset txn flag
            txn_flag = 0
            stmt = ""
            continue

        if txn_flag == 1:
            continue


        ## Single statement processing...
        stmt = stmt[0:(stmt.rfind(";"))]

        raw_stmt = stmt
        
        qstr = "curl -s \'" + host + ":" + port + "/"
        stmt = stmt.replace("\n", " ")
        stmt = stmt.replace("\\", "\\\\")
        stmt = stmt.strip()
        cmd, *args = shlex.split(stmt)

        if cmd.upper()=='SELECT':
            if CONSISTENCY == "strong":
                # need to do a quorum read or read from proposer of latest block
                # can use the /validators endpoint
                output = os.popen("curl -s \'" + host + ":" + port + "/validators\'").read()
                y = json.loads(str(output))
                validators = y["result"]["validators"]

                # get all validators
                proposer_priority = float("-inf")
                proposer = 0
                for i, validator in enumerate(validators):
                    if float(validator["proposer_priority"]) > proposer_priority:
                        proposer_priority = float(validator["proposer_priority"])
                        proposer = validator["address"]

                proposer_port = val_map[proposer]
                #print(proposer_port)
                qstr = "curl -s \'" + host + ":" + proposer_port + "/"

            tmp = {}
            tmp['data'] = "\"" + stmt + "\""
            enc = urllib.parse.urlencode(tmp)
            qstr += "abci_query?" + enc + "\'"

            qstr = qstr.replace("%E2%80%9C", "%5C%22")
            qstr = qstr.replace("%E2%80%9D", "%5C%22")
            #print(qstr)
            output = os.popen(qstr).read()
            y = json.loads(str(output))
            #print(y)
            if 'error' in y:
                print(y["error"]["message"], ":", y["error"]["data"])
            else:
                resp = y["result"]["response"]["info"]
                resp_dict = json.loads(resp)
                df = pd.DataFrame.from_dict(resp_dict)
                print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))



        elif cmd.upper()=='INSERT' or cmd.upper()=='UPDATE' or cmd.upper()=='DELETE' or cmd.upper()=='CREATE' or cmd.upper()=='TRUNCATE':
            tmp = {}
            tmp['tx'] = "\"" + stmt + "\""
            enc = urllib.parse.urlencode(tmp)
            
            qstr += "broadcast_tx_async?" + enc + "\'"

            qstr = qstr.replace("%E2%80%9C", "%5C%22")
            qstr = qstr.replace("%E2%80%9D", "%5C%22")

            #print(qstr)
            # async req
            output = os.popen(qstr).read()
            y = json.loads(str(output))
            if not 'error' in y:
                txnstrs.append(raw_stmt)


        else:
            print('Unknown command: {}'.format(cmd))

        stmt = ""
        sql_stmt = sqlfile.readline()

    sqlfile.close()

    search_txns(txnstrs)
    return

if __name__ == "__main__":
    start = time.perf_counter()
    main(sys.argv[1:])
    end = time.perf_counter()
    print("Timing with " + sys.argv[2] + " nodes: " + str(end-start) + " seconds.")

