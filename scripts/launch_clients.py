#!/usr/bin/env python
#
# This script starts (total_clients / num_client_vms) client processes on the machine in which it is
# executed. The initial id of the servers (passed in using the -i flag) is used to load balance clients
# among the servers in a given VM.
#
#
# Sample invocation (from the root folder of Pung):
# scripts/launch_clients.py --ip [ip] -s 1 -c 8 --svm 1 --cvm 8 -i 0 -k 64 -o h2 -t b -n 1000 -n 2000
#
# Result
# This launches 8 client processes that connect to 1 server process at the given ip.
# Clients will register with the server and will send and receive messages for a total of 
# r rounds (default is 10). The results will be stored in the output folder (default is ".").
# The Pung servers will simulate (n = 1000 - 8) and (n = 2000 -8) clients (these are two different 
# experiments, and you may pass fewer or more).
# This whole experiment will be repeated --trial times (default is 1). 
# The PIR and retrieval parameters (k, d) need to be the same as those passbed by the servers.


import os
import sys
import time
import argparse


parser = argparse.ArgumentParser()

parser.add_argument('-s', dest='total_servers', default=1, help='total servers', type=int)
parser.add_argument('-c', dest='total_clients', default=1, help='total clients', type=int)
parser.add_argument('--svm', dest='num_server_vms', default=1, help='number of server VMs', type=int)
parser.add_argument('--cvm', dest='num_client_vms', default=1, help='number of client VMs', type=int)
parser.add_argument('-i', dest='id', default=0, help='id of server', type=int)
parser.add_argument('-k', dest='rate', default=1, help='retrieval rate', type=int)
parser.add_argument('-o', dest='opt', default='', help='optimization', type=str)
parser.add_argument('-n', dest='num', action='append', default=[], help='num messages', type=int)
parser.add_argument('--trial', dest='trial', default=1, help='num trials', type=int)
parser.add_argument('-r', dest='rounds', default=10, help='num rounds', type=int)
parser.add_argument('--ip', dest='ip', default='127.0.0.1', help='server ip', type=str)
parser.add_argument('-p', dest='port', default=8000, help='server initial port', type=int)
parser.add_argument('-d', dest='d', default=2, help='PIR recursion depth', type=int)
#parser.add_argument('-a', dest='a', default=8, help='PIR alpha', type=int)
parser.add_argument('-t', dest='ret', default='b', help='retrieval type', type=str)
parser.add_argument('--out', dest='out', default='.', help='out folder', type=str)

results = parser.parse_args()

num_messages = results.num

if (len(num_messages) == 0):
  num_messages.append(131072) # default number of messages

total_servers = results.total_servers
total_clients = results.total_clients

num_server_vms = results.num_server_vms
num_client_vms = results.num_client_vms

num_trials = results.trial
vm_id = results.id

server_ip = results.ip

rate = results.rate

rounds = results.rounds
pir_d = results.d
#pir_a = results.a
ret = results.ret
out = results.out
init_port = results.port

opt = ""
opt_out = ""

if (results.opt != ''):
  opt = " -o " + results.opt
  opt_out = "_" + results.opt

clients_per_server = total_clients // total_servers  # ratio of clients to servers (to load balance things)
servers_per_vm = total_servers // num_server_vms     # number of server processes for each VM
clients_per_vm = total_clients // num_client_vms

init_server_id = vm_id * servers_per_vm
postfix = str(num_client_vms) + "vm"

for i in num_messages:
  for trial in range(num_trials):
    for client in range(clients_per_vm):

      # Ask each server to generate extra tuples (to ensure we meet all the num_messages)
      extra = (i - total_clients) // total_servers

      command = "./target/release/client -n " + str(client) + " -p " + str(client) + " -x \"hahahahe\""
      command += " -h " + str(server_ip) + ":" + str(init_port + init_server_id + (client % servers_per_vm))
      command += " -d " + str(pir_d) + " -r " + str(rounds) + " -b " + str(extra)
      command += " -k " + str(rate) + opt + " -t " + ret
      command += " >> " + out + "/" + str(i) + "_" + str(total_servers) + "s_" + str(total_clients) + "c_"
      command += str(postfix) + "_" + str(rate) + "k" + opt_out + ".log"

      if (client != clients_per_vm - 1):
        command += " &"

      print(command)
      os.system(command)

    time.sleep(1)
