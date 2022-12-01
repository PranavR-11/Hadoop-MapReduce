import socket
import subprocess
import os, sys

def map_shuffle_reduce(file_name, num_workers):
    new_file_name = file_name[:-1] #name540
    print(new_file_name)

    prt = 0

    shuffled_file_list = []

    while prt < int(num_workers):
        prt = prt + 1
        file_name = new_file_name + str(prt) #name5401
        file_descr = open(file_name, 'r')
        Lines = file_descr.readlines()

        mapper_file_descr = open(file_name+'_mapped', 'w+') #name5401_mapped
        mapper_file_descr.close()
        mapper_file_descr = open(file_name+'_mapped', 'a')
        mapped_file_name  = file_name+'_mapped'
        punctmarks = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
        for l1 in Lines:
            for wo1 in l1:
                if wo1 in punctmarks:
                    l1 = l1.replace(wo1, "")
            l1 = l1.strip()
            wos = l1.split()
            for wo in wos:
                #print("%s\t\t%s" % (wo, 1))
                mapper_file_descr.write("%s\t\t%s\n" % (wo, 1))
        mapper_file_descr.close()

        print('mapped file '+ mapped_file_name)
        file_descr = open(mapped_file_name, 'r')
        Lines = file_descr.readlines()
        no_of_nodes = int(num_workers)
        
        for line in Lines:
            x=ord(line[0])%no_of_nodes
            print('mod ' + str(x) + 'for ' + line[0])
            shuffle_file_name = 'shuffled'+ str(x) + "p" #name54010p
            print('shuffled file ' + shuffle_file_name)

            if shuffle_file_name not in shuffled_file_list:
                shuffled_file_list.append(shuffle_file_name)
            
                shuffle_file_descr = open(shuffle_file_name, 'w+')
                shuffle_file_descr.close()
            temp_file_descr = open(shuffle_file_name, "a")
            #print("%s\n" % (line))
            temp_file_descr.write("%s" % (line))
            temp_file_descr.close()
        mapper_file_descr.close()

    reducer_file_descr_all = open(file_name +'_all_reduced', 'w+') #name5401_all_reduced
    reducer_file_descr_all.close()
    reducer_file_descr_all = open(file_name+'_all_reduced', 'a')
    prt = 0
    for fname in shuffled_file_list:
        file_name = new_file_name + '1'
        shuffle_file_name = file_name + str(x)+"p"

        cur_wor = None
        cur_cou = 0
        wo = None

        words_list = []
        words_count = []
        shuffle_file_name = fname #name54010p_reduced
        reducer_file_descr = open(shuffle_file_name+'_reduced', 'w+')
        reducer_file_descr.close()
        reducer_file_descr = open(shuffle_file_name+'_reduced', 'a')
        reducer_file_name  = shuffle_file_name+'_reduced'
        file_descr = open(shuffle_file_name, 'r')
        print('reduced '+ reducer_file_name)
        Lines = file_descr.readlines()
        for l1 in Lines:
            l1 = l1.strip()
            wo, count = l1.split('\t', 1)
            try:
                count = int(count)
            except ValueError:
                continue

            if wo in words_list:
                words_count[words_list.index(wo)] += 1
                cur_cou += count
            else:
                words_list.append(wo)
                cur_cou = count
                cur_wor = wo
                print(words_list.index(wo))
                words_count.append(1)

        for word in words_list:
            #print ("%s\t\t %s" % (cur_wor, cur_cou))
            reducer_file_descr.write("%s\t\t %s\n" % (word, words_count[words_list.index(word)]))
            reducer_file_descr_all.write("%s\t\t %s\n" % (word, words_count[words_list.index(word)]))
        reducer_file_descr.close()
        prt = prt + 1

    reducer_file_descr_all.close()


n = len(sys.argv)
print("Total arguments passed:", n)
print(sys.argv[1])
port = int(sys.argv[1])
print(port)



def server_program():
    # get the hostname
    host = socket.gethostname()
    #port = 5400  # initiate port no above 1024

    print('In worker')
    while True:
        print('Starting Worker Connections...')
        server_socket = socket.socket()  # get instance
        # look closely. The bind() function takes tuple as argument
        server_socket.bind((host, port))  # bind host address and port together
        print('Worker Bound to '+str(port))
        # configure how many client the server can listen simultaneously
        print('Worker Listening...')
        server_socket.listen(2)
        conn, address = server_socket.accept()  # accept new connection
        print("Worker got Connection from: " + str(address))
        while True:
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                # if data is not received break
                break
            print("from connected user: " + str(data))
            pie = '0'
            words = data.split("!7zX")
            if words[0] == 'WR':
                file_descr = open(words[1], 'w+')
                file_descr.write(words[2])
                file_descr.close()
            elif words[0] == 'RD':
                file_descr = open(words[1], 'r')
                pie = file_descr.read()
                file_descr.close()
                print(pie)
            elif words[0] == 'MR':
                map_shuffle_reduce(words[1], words[2])
                file_descr = open(words[1], 'r')
                print(pie)
            #data = raw_input(' -> ')
            data = pie
            conn.send(data.encode())  # send data to the client
        print('Client connection closed')
        conn.close()  # close the connection

if __name__ == '__main__':
    server_program()

