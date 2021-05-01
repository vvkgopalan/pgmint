import sys, getopt
import shutil
import os
import subprocess
import signal
import re
import time
import json

from subprocess import Popen, PIPE

def kill_pg(n_nodes):
   ports = [i for i in range(5432-n_nodes+1, 5433)]
   ports = [str(port) for port in ports]

   for port in ports:
      process = Popen(["lsof", "-i", "tcp:{0}".format(port)], stdout=PIPE, stderr=PIPE)
      stdout, stderr = process.communicate()
      for process in str(stdout.decode("utf-8")).split("\n")[1:]:       
         data = [x for x in process.split(" ") if x != '']
         if (len(data) <= 1):
            continue

         os.kill(int(data[1]), signal.SIGKILL)

def gen_config(n_nodes, dirname):
   process = Popen(["pwd"], stdout=PIPE, stderr=PIPE)
   stdout, stderr = process.communicate()
   wd = str(stdout.decode("utf-8"))

   # cd into dir
   process = Popen(["cd", dirname], stdout=PIPE, stderr=PIPE)
   stdout, stderr = process.communicate()
   os.environ["TMHOME"] = "./" + dirname + "/tmp"
   tminit = ['tendermint', 'init', 'validator']
   process = Popen(tminit, stdout=PIPE, stderr=PIPE)
   stdout, stderr = process.communicate()
   for line in str(stdout.decode("utf-8")).split("\n"):
      print(line)

   with open("tmp/config/genesis.json") as gfile:
      genesis = json.load(gfile)
   with open("tmp/config/priv_validator_key.json") as vfile:
      validator = json.load(vfile)
   with open("tmp/config/node_key.json") as nfile:
      node_key = json.load(nfile)

   # cd back
   process = Popen(["cd", wd], stdout=PIPE, stderr=PIPE)
   stdout, stderr = process.communicate()

   return genesis, validator, node_key




def destroy(n_nodes, dirname):

   ## kill postgres processes that may be running (POTENTIALLY DANGEROUS)
   ## Confirm that PG processes are running on ports 5432 to 5432+n_nodes
   kill_pg(n_nodes)
   time.sleep(n_nodes) # cleaning up

   tmp_path = dirname + "/tmp"
   data_path = dirname + "/data"
   pgdata_path = dirname + "/pgdata"
   log_path = dirname + "/logfile"

   # cleanup existing files
   try:
      shutil.rmtree(tmp_path)
   except OSError as e:
      print("Error: %s : %s" % (tmp_path, e.strerror))

   try:
      shutil.rmtree(data_path)
   except OSError as e:
      print("Error: %s : %s" % (data_path, e.strerror))

   try:
      shutil.rmtree(pgdata_path)
   except OSError as e:
      print("Error: %s : %s" % (pgdata_path, e.strerror))

   try:
      os.remove(log_path)
   except OSError as e:
      print("Error: %s : %s" % (log_path, e.strerror))

   for i in range(n_nodes - 1):
      sink = dirname + str(i+1)
      ## cleanup directory if exists
      try:
         shutil.rmtree(sink)
      except OSError as e:
         print("Error: %s : %s" % (sink, e.strerror))

def start(n_nodes, dirname):

   ## kill postgres processes that may be running (POTENTIALLY DANGEROUS)
   ## Confirm that PG processes are running on ports 5432 to 5432+n_nodes
   kill_pg(n_nodes)
   time.sleep(n_nodes) # cleaning up

   tmp_path = dirname + "/tmp"
   data_path = dirname + "/data"
   pgdata_path = dirname + "/pgdata"
   log_path = dirname + "/logfile"

   # cleanup existing files
   try:
      shutil.rmtree(tmp_path)
   except OSError as e:
      print("Error: %s : %s" % (tmp_path, e.strerror))

   try:
      shutil.rmtree(data_path)
   except OSError as e:
      print("Error: %s : %s" % (data_path, e.strerror))

   try:
      shutil.rmtree(pgdata_path)
   except OSError as e:
      print("Error: %s : %s" % (pgdata_path, e.strerror))

   try:
      os.remove(log_path)
   except OSError as e:
      print("Error: %s : %s" % (log_path, e.strerror))

   ## now make copies of the source directory
   netconf = {}
   for i in range(n_nodes - 1):
      sink = dirname + str(i+1)
      ## cleanup directory if exists
      try:
         shutil.rmtree(sink)
      except OSError as e:
         print("Error: %s : %s" % (sink, e.strerror))

      ## now copy src to sink
      try:
         destination = shutil.copytree(dirname, sink) 
         print("Copied to destination " + destination)
      except OSError as e:
         print("Error: %s to %s : %s" % (dirname, sink, e.strerror))

      # cleanup netconf
      netconf_path = sink + "/netconf.json"
      try:
         os.remove(netconf_path)
      except OSError as e:
         print("Error: %s : %s" % (netconf_path, e.strerror))

      netconf['dbhost'] = "localhost"
      netconf['dbport'] = str(5432-(i+1))
      with open(netconf_path, 'w') as fp:
         json.dump(netconf, fp)


   ## now start postgres instances
   for i in range(n_nodes):
      if i == 0:
         sink = dirname + "/pgdata"
      else:
         sink = dirname + str(i) + "/pgdata"

      command = 'initdb -D ' + sink 
      process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
      output, error = process.communicate()
      if error:
         print(error)
         return

      pnum = str(5432 - i)
      logpath = dirname + "/logfile"
      command = ['pg_ctl', '-D', sink, '-o', '-p "' + pnum + '"', '-l', logpath, 'start']
      process = Popen(command, stdout=subprocess.PIPE)
      output, error = process.communicate()
      if error:
         print(error)


   genesis, validator, node_key = gen_config(n_nodes, dirname)


   #tm_init(n_nodes, dirname)

   #start_nodes(n_nodes, dirname)




def main(argv):
   code = argv[0]
   dirname = argv[1]
   n_nodes = int(argv[2])

   if n_nodes < 0 or n_nodes > 9:
      print("Invalid Node Size: " + str(n_nodes))
      return 1

   if code.upper() == "START":
      print("Starting " + str(n_nodes) + " nodes.")
      start(n_nodes, dirname)
   elif code.upper() == "DESTROY":
      print("Destroying " + str(n_nodes) + " nodes.")
      destroy(n_nodes, dirname)
   else:
      print("Invalid Command: " + str(code))
      return 1

   return 0
   

if __name__ == "__main__":
   main(sys.argv[1:])