import readline
import shlex
import urllib.parse
import os
import json
import subprocess
import pandas as pd
from tabulate import tabulate

print('Enter a PSQL query.')

port = '26657'
host = 'localhost'

while True:
    stmt = input('> ')
    if stmt == "":
        continue

    cmd, *args = shlex.split(stmt)
    qstr = "curl -s \'" + host + ":" + port + "/"
    stmt = stmt.replace(";", "")
    stmt = stmt.replace("\"", "\\\"")

    if cmd.upper()=='EXIT':
        break

    elif cmd.upper()=='HELP':
        # ...
        print('Enter a PSQL query.')

    elif cmd.upper()=='INFO':
        output = os.system("curl -s \'" + host + ":" + port + "/abci_info\'")
        print(output)

    elif cmd.upper()=='SELECT':
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

