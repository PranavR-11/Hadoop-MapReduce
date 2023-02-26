import socket
import os
import math
from contextlib import contextmanager
import time

@contextmanager
def file_pies(filename, pie_size):
    f = open(filename, 'rb')
    try:
        def gen():
            b = f.read(pie_size)
            while b:
                yield b
                b = f.read(pie_size)
        yield gen()
    finally:
        f.close()

host=socket.gethostname()

def send_to_worker(worker_port, filename, pie):
    ws=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    print(worker_port)
    ws.connect((host,worker_port))
    sendstring = 'WR!7zX' + filename + '!7zX' + pie.decode()
    ws.send(sendstring.encode())
    ws_messg=ws.recv(1024)
    print("message from worker: ", ws_messg.decode())
    ws.close()

def read_from_worker(worker_port, filename):
    ws=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    print(worker_port)
    ws.connect((host,worker_port))
    sendstring = 'RD!7zX' + filename
    ws.send(sendstring.encode())
    pie =ws.recv(1024).decode()
    print("message from worker: ",pie)
    ws.close()
    return pie
    
#change1: extra function added 

def send_for_mapper_to_worker(worker_port, filename, num_workers):
    ws=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    print(worker_port)
    ws.connect((host,worker_port))
    sendstring = 'MR!7zX' + filename + '!7zX' + str(num_workers)
    ws.send(sendstring.encode())
    ws_messg=ws.recv(1024)
    print("message from worker: ",ws_messg.decode())
    ws.close()

    
    
con_closed = 0
#Connect to the server
port=7634
#port=5401
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.connect((host,port))

while True:
    op = int(input("Choose which operation you want to execute:\n1.Write\n2.Read\n3.Map-Reduce\n"))

    if con_closed == 1:
        port=7634
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((host,port))
    if op == 1:
        #message = "Write"
        worker_nodes = int(input("How many worker nodes do you want?:\n"))
        file_name = input("\nEnter name of the file you want to write:\n")
        print("\nFile "+file_name+" will be written by being divided amongst "+str(worker_nodes)+" worker nodes.")
        c_messg = 'WR,'+str(worker_nodes)+','+file_name
        s.send(c_messg.encode())
        print("wating for response")
        s_messg=s.recv(1024)
        print("message from server: ",s_messg.decode())
        num_workers = int(s_messg.decode())
        time.sleep(1)
        if num_workers > 0:
            if len(file_name) > 1:
                file_size = os.path.getsize(file_name)
            pie_size = int(math.ceil(file_size/num_workers))
            starting_port = 5401
            with file_pies(file_name, pie_size) as pies:
                for pie in pies:
                    # write pie to worker
                    send_to_worker(starting_port, file_name+str(starting_port),pie)
                    starting_port = starting_port + 1                
            print('Done writing to ' + str(num_workers) + ' workers')        
    elif op == 2:
        message = "Read"
        file_name = input("\nEnter name of the file you want to read:\n")
        c_messg = 'RD,'+file_name
        s.send(c_messg.encode())
        print("wating for response")
        s_messg=s.recv(1024)
        print("message from server: ",s_messg.decode())
        num_workers = int(s_messg.decode())
        prt = 0
        starting_port = 5401
        file_descr = open(file_name, 'w+')
        file_descr.close()
        file_descr = open(file_name, 'a')
        pie=' '
        if num_workers > 0:
            while prt < num_workers:
                prt = prt + 1
                pie = read_from_worker(starting_port, file_name+str(starting_port))
                file_descr.write(pie)
                starting_port = starting_port + 1
        file_descr.close()
        print('Done reading from ' + str(num_workers) + ' worker')
    elif op == 3: #change added in this
        c_messg = 'MR,file_name'
        s.send(c_messg.encode())
        print("wating for response")
        s_messg=s.recv(1024)
        print("message from server: ",s_messg.decode())
        num_workers = int(s_messg.decode())
        print(num_workers)
        starting_port= 5401
        pie=' '
        if num_workers > 0:
            pie = send_for_mapper_to_worker(starting_port, file_name+str(starting_port), num_workers)
    else:
        message = "Please select one operation"
    con_closed = 1
    print('Connection to Master closed')
    s.close()

