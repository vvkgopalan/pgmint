import readline
import shlex
import urllib.parse
import os
import json
import subprocess
import pandas as pd
from tabulate import tabulate

print('Enter a PSQL query.')

port = '26657' #tm port
host = 'localhost' #tm host
dbhost = 'localhost' #db host
dbport = '5432' #db port

CONSISTENCY = "strong" # read consistency
N_NODES = 1 # number of nodes in network
SRC = "src" # directory with source code

## Hacky, but first discover all validators addresses
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

    val_map[addr] = str(26657+10*i) # Hard Coded Addr.

stmt = ""
txn_flag = 0
while True:
    tmp_stmt = input('> ')
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

        # Convert to URI/HTTP - a REST like interface for the backend tendermint RPC
        qstr = "curl -s \'" + host + ":" + port + "/"
        stmt = stmt.replace("\n", " ")
        stmt = stmt.replace("\"", "\\\"")
        stmt = " ".join(stmt.split())

        # create txn
        tmp = {}
        tmp['tx'] = "\"" + stmt + "\""
        enc = urllib.parse.urlencode(tmp)
        qstr += "broadcast_tx_commit?" + enc + "\'"

        qstr = qstr.replace("%E2%80%9C", "%5C%22") # weird encoding issues
        qstr = qstr.replace("%E2%80%9D", "%5C%22")

        print(qstr)
        output = os.popen(qstr).read()
        print(output)
        y = json.loads(str(output))

        if 'error' in y:
            print(y["error"]["message"], ":", y["error"]["data"])
        else:
            if (int(y["result"]["check_tx"]["code"]) != 0):
                print("Check TX Log: ", y["result"]["check_tx"]["log"])
            
            if (int(y["result"]["deliver_tx"]["code"]) != 0):
                print("Deliver TX Log: ", y["result"]["deliver_tx"]["log"])
            else:
                print(y["result"]["deliver_tx"]["log"])


        # reset txn flag
        txn_flag = 0
        stmt = ""
        continue

    if txn_flag == 1:
        continue


    ## Single statement processing...
    stmt = stmt[0:(stmt.rfind(";"))]
    
    qstr = "curl -s \'" + host + ":" + port + "/"
    stmt = stmt.replace("\n", " ")
    stmt = stmt.replace("\"", "\\\"")
    stmt = " ".join(stmt.split())
    stmt = stmt.strip()
    cmd, *args = shlex.split(stmt)

    if cmd.upper()=='EXIT':
        break

    elif cmd.upper()=='HELP':
        # ...
        print('Enter a PSQL query.')


    elif stmt[0] == "\\":
        # metacommand
        # make a psql call using psql -E
        # TODO
        print('Metacommands currently unsupported.')

    elif cmd.upper()=='INFO':
        output = os.popen("curl -s \'" + host + ":" + port + "/abci_info\'").read()
        print(output)

    elif cmd.upper()=='SELECT':
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
        if 'error' in y:
            print(y["error"]["message"], ":", y["error"]["data"])
        else:
            resp = y["result"]["response"]["info"]
            resp_dict = json.loads(resp)
            df = pd.DataFrame.from_dict(resp_dict)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))



    elif cmd.upper()=='INSERT' or cmd=='UPDATE' or cmd=='DELETE' or cmd=='CREATE' or cmd=='TRUNCATE':
        tmp = {}
        tmp['tx'] = "\"" + stmt + "\""
        enc = urllib.parse.urlencode(tmp)
        qstr += "broadcast_tx_commit?" + enc + "\'"

        qstr = qstr.replace("%E2%80%9C", "%5C%22")
        qstr = qstr.replace("%E2%80%9D", "%5C%22")

        print(qstr)
        output = os.popen(qstr).read()
        print(output)
        y = json.loads(str(output))

        if 'error' in y:
            print(y["error"]["message"], ":", y["error"]["data"])
        else:
            if (int(y["result"]["check_tx"]["code"]) != 0):
                print("Check TX Log: ", y["result"]["check_tx"]["log"])
            
            if (int(y["result"]["deliver_tx"]["code"]) != 0):
                print("Deliver TX Log: ", y["result"]["deliver_tx"]["log"])
            else:
                print(y["result"]["deliver_tx"]["log"])


    else:
        print('Unknown command: {}'.format(cmd))

    stmt = ""

