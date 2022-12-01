import socket
import subprocess
import os
import signal

def server_program():
    ret = 0
    cur_worker_count = 0
    next_worker_port = 5400
    meta_data_file_name = []
    meta_data_num_workers = []
    # get the hostname
    host = socket.gethostname()
    #port = 5400  # initiate port no above 1024
    port = 7634
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    while True:
        # configure how many client the server can listen simultaneously
        server_socket.listen(2)
        conn, address = server_socket.accept()  # accept new connection
        print("Connection from: " + str(address))
        ret_a = [] 
        while True:
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                # if data is not received break
                break
            print("Master node got from Client: " + str(data))
            sdata = 'none'
            words = data.split(',')
            if words[0] == 'WR':
                num_workers = int(words[1])
                file_name = words[2]

                if num_workers > cur_worker_count: 
                    try:
                        prt = 0
                        gap = num_workers - cur_worker_count
                        while prt < gap:
                            prt = prt + 1
                            next_worker_port = next_worker_port + 1
                            cur_worker_count =  cur_worker_count + 1
                            #cmd = "python /home/ubuntu/.trials/WORKER_2.py %s" %next_worker_port
                            cmd = "python3 /home/pes2ug20cs248/Desktop/BD_proj/WORKER_1.py %s" %next_worker_port
                            print(cmd)
                            ret = subprocess.Popen(cmd, shell=True)
                            ret_a.append(ret)
                    except subprocess.CalledProcessError as e:
                        if e.returncode > 1:
                            print(e)
                            raise
                err = 0
                try:
                    index = meta_data_file_name.index(file_name)
                except ValueError:
                    print('New file, adding to list')
                    err = 1
                    meta_data_file_name.append(file_name)
                    meta_data_num_workers.append(num_workers)
                sdata = str(num_workers)
                if err == 0:
                  meta_data_num_workers[index] = num_workers
            elif words[0] == 'RD' or words[0] == 'MR':
                try:
                    index = meta_data_file_name.index(file_name)
                except ValueError:
                    index = -1
                file_name = words[1]

                if index != -1:
                    sdata = str(meta_data_num_workers[index])
                else:
                    sdata = '0'
    
            #data = raw_input(' -> ')
            print(sdata)
            conn.send(sdata.encode())  # send data to the client

    if ret != 0:
        for ret in ret_a:
            print('Terminating worker')
            os.killpg(os.getpgid(ret.pid), signal.SIGTERM)
    print('Terminating Master Connections')
    conn.close()  # close the connection


if __name__ == '__main__':
    server_program()

