import sys, getopt
import shutil

def start(n_nodes, dir):
   for i in range(n_nodes):
      tmp_path = dirname + "/tmp"
      data_path = dirname + "/data"
      pgdata_path = dirname + "/pgdata"
      log_path = dirname + "/logfile"
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
         shutil.rmtree(log_path)
      except OSError as e:
         print("Error: %s : %s" % (log_path, e.strerror))



def main(argv):
   code = argv[0]
   dirname = argv[1]
   n_nodes = argv[2]

   if n_nodes < 0 or n_nodes > 9:
      print("Invalid Node Size: " + str(n_nodes))
      return 1

   if code.upper() == "start":
      start(n_nodes, dirname)
   elif code.upper() == "destroy":
      destroy(n_nodes, dirname)
   else:
      print("Invalid Command: " + str(code))
      return 1

   return 0
   

if __name__ == "__main__":
   main(sys.argv[1:])