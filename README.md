#Hadoop-MapReduce
The project depicts implementation of the core components of Hadoop's Map Reduce Framework.
The aim of this project was to gain a deeper understanding on how MapReduce jobs are executed parallelly across multiple nodes. This is achieved by creating a setup of a multi-node configuration that can store input data across multiple nodes and run Map and Reduce jobs.
Python has been used along with socket programming for the connection between the multi-nodal environment.
This can be run on ubuntu (python3) and not windows.
The components of the server include the following:

•	Client Node

  The client node interacts with the user on what operation does he/she want to do, followed by taking inputs such as desired number of worker nodes and input file.
  
•	Master Node

  The master node accepts the operations from the client and coordinates between the worker nodes to complete the tasks.
  
•	Worker Node(s)

  Running the tasks parallely and storing the input data partitions.
    
The pictorial representation of the setup:

![image](https://user-images.githubusercontent.com/94732433/205029000-beaaba02-b78f-4008-b2e2-0baaf3e0ccdb.png)

The three operations the user will be given a choice of are:

• Write

  The write operation takes input from the user on what file do they want to write and how many worker nodes (n) so that the file is divided into n different partitions and stored in n separate files.
  
• Read

  The read operation takes input of the file to be read which displays all the worker nodes' content that were used to write it in order with the help of meta-data.
  
• Map-Reduce Operation

  The Map-Reduce operation consists of three steps: Mapping, Shuffling followed by Reducing of the partitioned files that were already created when Writing of the file was done.
  
These functions are applied on each of the partitioned files and performs a word count on the files eventually combining and giving the final output of word count in the entire file.

The mapper will create a new intermediate file(s) as its output which will be taken as an input for the shuffle function which will perform the shuffle operation on these files through hashing and then give another output file(s) which will be given as input for the reducer function, the reducer will perform the distinct word count on each of the files and the output of all the files (due to reduce here) will be combined to form a single file which will be the final output.



