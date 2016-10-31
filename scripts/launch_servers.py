#!/usr/bin/env python
#
# This script starts (total_servers/ num_server_vms) server processes on the machine in which it is 
# executed. The initial id of the servers (passed using the -i flag and incremented for each 
# server process) identifies each server. This id is used by client processes (see launch_clients.py).
# Since Pung relies on Timely dataflow for distribution, a host file needs to be provided listing
# all the timely dataflow workers ips and ports (see the provided host_file.txt for an example).
#
# Sample invocation (from the root folder of Pung):
# scripts/launch_servers.py -f host_file.txt -s 1 -c 8 --svm 1 --cvm 8 -i 0 -k 64 -o h2 -t b
# 
# Result:
# This launches 1 server (-s / --svm) that expects 8 clients. The Pung server is configured to support 
# (-k 64) simultaneous retrievals using the hybrid scheme that tolerates up to 2 collisions per 
# bucket (-o h2). The retrieval procedure uses a bloom filter to map labels to indices (-t b).


import os
import sys
import socket
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('-s', dest='total_servers', default=1, help='total servers', type=int)
parser.add_argument('-c', dest='total_clients', default=1, help='total clients', type=int)
parser.add_argument('--svm', dest='num_server_vms', default=1, help='number of server VMs', type=int)
parser.add_argument('--cvm', dest='num_client_vms', default=1, help='number of client VMs', type=int)
parser.add_argument('-i', dest='id', default=0, help='id of server', type=int)
parser.add_argument('-k', dest='rate', default=1, help='retrieval rate', type=int)
parser.add_argument('-o', dest='opt', default='', help='optimization', type=str)
parser.add_argument('-t', dest='ret', default='b', help='retrieval type', type=str)
parser.add_argument('-d', dest='d', default=2, help='PIR recursion depth', type=int)
parser.add_argument('-f', dest='host_file', default='scripts/hosts.txt', help='host file for Pung servers', type=str)
parser.add_argument('-p', dest='port', default=8000, help='server initial port', type=int)

results = parser.parse_args()


ip = socket.gethostbyname(socket.gethostname())

total_servers = results.total_servers
total_clients = results.total_clients

num_server_vms = results.num_server_vms
num_client_vms = results.num_client_vms
vm_id = results.id

clients_per_server = total_clients // total_servers
servers_per_vm = total_servers // num_server_vms

init_server_id = vm_id * servers_per_vm

rate = results.rate
ret = results.ret
pir_d = results.d
host_file = results.host_file
init_port = results.port

opt = ""

if (results.opt != ''):
  opt = " -o " + results.opt

for i in range(init_server_id, init_server_id + servers_per_vm):
  server_command = "./target/release/server -i " + str(ip) + " -s " + str(init_port) + " -d " + str(pir_d)
  server_command += " -t " + str(ret) + " -m " + str(clients_per_server)
  server_command += " -h " + host_file + " -n " + str(total_servers)
  server_command += " -p " + str(i) + " -k " + str(rate) + opt

  if (i != init_server_id + servers_per_vm - 1):
    server_command = server_command + " &"

  print(server_command)
  os.system(server_command)
