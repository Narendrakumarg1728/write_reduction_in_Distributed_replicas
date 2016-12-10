#Replica program performing the task of the proxy to handel the connection and to store it in a key value database pickel DB

import sys, socket, subprocess, pickledb, threading, time 

#global counter 
#counter = 0
#global r1 = pickledb.load("replica", False)

def set_func(key, value, time_id):                                          #Function to set the key and value to DB
    #global r1 = pickledb.load("replica", False)
#    global counter
#    counter = counter +1
#    print ("Num of writes till now: %d" %(counter))
    print("Debug: In set function")
    print("setiing key: " + key + "with value: " + value)
    try:
        success_set =  r1.set(key, value)
        print ("Debug: Value of the return of set: %s" %(success_set))
        writes = 1
        if ((writes == 1)):
         # write_dict[writes] = counter
            f1=open('./number_of_writes', 'a')
            out_put = str(writes) + ","            #+ str(counter)
            f1.write(out_put)
            f1.close
            writes =0
        return (success_set)
    except:
        success_set = "failure not able to set the key and value"
        print (success_set)
        return (success_set)

def get_func(key):                                                          #Function to ge the value for the given key from the DB
    print("Debug: In get function")
    print ("Getting key: " + key + "value from replica1 ")
    try:
        success_get = (r1.get(key))
        print ("Debug: Value of the return of get: " + success_get)
        return (success_get)
    except:
        success_set = "failure not able to get the key and value"
        print (success_set)
        return (success_set)

def log_timer_func(t):
    global time_out 
    while True:
#        print('Timer running in back ground')
        while (t >= 0 and time_out == 1):
            time.sleep(1)
            t -= 1
#        print('\n \n \n \n \n Logger Alarm, time to send update to all! \n \n \n \n \n')
        time_out = 0



def replicate_func(socket):
    conn1, addr1 = socket.accept()
    BUFSIZE = 1024
    print ("Debug: Inside Broad_cast_func")
    while True:
        print ('New connection from %s:%d' % (addr1[0], addr1[1]))
        data = conn1.recv(BUFSIZE)
        command = data.split()
        if not data:
            break
        elif command[0] == 'exit':
            conn1.send('\0')
            quit(conn)
        else:
            print(data)
        for i in command:
            print ("Debug: Value of command received:")
            print (i)
            
        if command[0] == 'set':                                #Handling set command
            key = command[1]
            value = command[2]
            time_id = command[3]
            #data_1 [key] = value
            #time_1 [key] = time_id
            #update = 1
           # print ("Debug: printing dict value: ")
           # print data_1
           # print time_1
            success = set_func(key, value, time_id)
            print ("%s", success)


def send_all(con_send, data_send):
    con_send.send(data_send)

host = ''
r1 = pickledb.load("replica", False)
port1 = int (sys.argv[1])
mode = sys.argv[2]
print ("\nPort provided is: %d\n" % port1)
print ("\nMode provided is: %s\n" % mode)


if  (port1 == 17201):                                                #Connecting to different Replicas.
        p1 = 17302
        p2 = 17402
        p3 = 17502
#        p4 = 17602
elif (port1 == 17301):
        p1 = 17202
        p2 = 17403
        p3 = 17503
#        p4 = 17603

elif (port1 ==17401):
        p1 = 17203
        p2 = 17303
        p3 = 17504
#        p4 = 17604
elif (port1 == 17501):
        p1 = 17204
        p2 = 17304
        p3 = 17404
#        p4 = 17605

elif ( port1 == 17601):
        p1 = 17205
        p2 = 17305
        p3 = 17405
#        p4 = 17505

print ("Esatablishing connection for replica: replica2")               # %s " %(replica1))
s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to replica 2
s1.bind((host, port1 + 1))
s1.listen(1)
thread = threading.Thread(target = replicate_func, args = (s1, ))
thread.start()
print("Started for receive on port for replica : %d "% (port1 + 1))

print ("Esatablishing connection for replica: replica3")               # %s " %(replica1))
s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to replica3 
s2.bind((host, port1 + 2))
s2.listen(1)
thread = threading.Thread(target = replicate_func, args = (s2, ))
thread.start()
print("Started for receive  on port: %d"% (port1 +2))

print ("Esatablishing connection for replica: replica4")               # %s " %(replica1))
s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to replica 4
s3.bind((host, port1 + 3))
s3.listen(1)
thread = threading.Thread(target = replicate_func, args = (s3, ))
thread.start()
print("Started on port: %d"% (port1 + 3))

#print ("Esatablishing connection for replica: replic5")               # %s " %(replica1))
#s4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to replica 2
#s4.bind((host, port1 + 4))
#s4.listen(1)
#thread = threading.Thread(target = replicate_func, args = (s4, ))
#thread.start()
#print("Started on port: %d"%(port1 + 4))

time.sleep (20)                                                       #Allowing all the replicas to be up before the clients start to send data... 

conn_send_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn_send_1.connect(('localhost',p1))
print ("Connecting to replica 1 with port: %d" %(p1))

conn_send_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn_send_2.connect(('localhost',p2))
print ("Connecting to replica 2 with port: %d" %(p2))

conn_send_3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn_send_3.connect(('localhost',p3))
print ("Connecting to replica 3 with port: %d" %(p3))

#conn_send_4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#conn_send_4.connect(('localhost',p4))
#print ("Connecting to replica 4 with port: %d" %(p4))

print ("Running at port number:%d to communicate to client" %(port1))
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to client
s.bind((host, port1))
print("Server started on port: %s"%port1)
s.listen(1)
print("Now listening from client...\n")
data_1 = { }
time_1 = { }
BUFSIZE = 1024
conn, addr = s.accept()                                              # Accept connection from the client
writes= 0
counter = 0
log_copy_dict = { }                                                    #Log for local replica used to flush data to all the replicas when running in lazy mode
global time_out 
timer = 60
time_out = 1

if mode == 'lazy_update':
    thread = threading.Thread(target = log_timer_func, args = (timer, ))
    thread.start()
    print("Started timer function for logger\n")

#write_dict= { }
while True:
    print 'New connection from %s:%d' % (addr[0], addr[1])
    data = conn.recv(BUFSIZE)
    print ("Debug: Connection successful")
    command = data.split()
    print command
    if not data:
        break
    elif command[0] == 'exit':
        conn.send('\0')
        quit(conn)

    else:    
        print(data)

        for i in command:
            print ("Debug: Value of command received:")
            print (i)

        if command[0] == 'set':                                #Handling set command
            key = command[1]
            value = command[2]
            time_id = command[3]
            #data_1 [key] = value
            #time_1 [key] = time_id
#            c.acquire()
#            if  update == 0:
            key_share = key
            value_share = value
            time_share = int (time_id)
            print ("Debug: Value of Key, value, time in shared variable: %s, %s, %d" % (key_share, value_share, time_share))
            print ("Debug: printing dict value: ")
            #print data_1
            #print time_1
            writes= writes +1                                             #Number of writes so far at this replica

            success = set_func(key, value, time_id)                       #Keeping in local DB and keeping track of time for future work

            if (mode == 'strong' ):                                  
                #counter = counter + 1
                print "Sending data to replica 1"
                #counter = counter + 1
                send_all(conn_send_1, data)
                print "Sending data to replica 2"
                #counter = counter + 1
                send_all(conn_send_2, data)
                print "Sending data to replica 3"
                # counter = counter + 1
                send_all(conn_send_3, data)
               
            elif ( mode == 'lazy_update'):                                     #In lazy update mode logging to local dict and then if only time out by a thread running in background send to all
                log_copy_dict[key] = value
                print ("Debug: time_out value: %d" % time_out)
                if time_out == 0:
                    print ("Log time out, time to send to all replicas\n")
                    time_id =  str (int (time.time()) )
                    for k,v in log_copy_dict.iteritems():
                        data = "set " + k + " " + v + " " + str(time_id)
                        print "Sending data to replica 1"
                        #counter = counter + 1
                        send_all(conn_send_1, data)
                        print "Sending data to replica 2"
                        #counter = counter + 1
                        send_all(conn_send_2, data)
                        print "Sending data to replica 3"
                        # counter = counter + 1
                        send_all(conn_send_3, data)

                    time_out == 1

       #     if ((writes == 1)):
               # write_dict[writes] = counter
        #        f1=open('./number_of_writes', 'a')
         #       out_put = str(writes) + ","            #+ str(counter)
          #      f1.write(out_put)
           #     f1.close
            #    writes =0
    #            f1.write(write_dict)


#            print ("Vaue of counter is: Total Number of COUNTS:%d" %(counter))
#            print "Sending data to replica 4"
#            send_all(conn_send_4, data)

            #update = 1
          #  c.release()

            out_put1 = "Value for key: " + key +  "is: " + value
            conn.send("success")
           # conn_1.send(data)
           # primary_data = conn_1.recv(BUFSIZE)

        elif command[0] == 'get':                              #Handling get command getting key value and displaying back to the client
            key = command[1]
            value = get_func(key)
            out_put1 = "Value for key: " + key +  "is: " + value
            conn.send(out_put1)

        else:
            out_put1 = "Not a proper command supported commands are set and get in this DB..."
            print out_put1
            conn.send(out_put1)

thread.join()
thread.join()
thread.join()
thread.join()
thread.join()

def quit(conn):
    conn.close()
    conn_send_1.close()
    conn_send_2.close()
    conn_send_3.close()

    quit(conn)

