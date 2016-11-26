#Server program performing the task of the proxy.
import sys, socket, subprocess, pickledb, threading, time

c = threading.Condition()
update = 0


def set_func(key, value):                                          #Function to set the key and value to DB
    print("Debug: In set function")
    print("setiing key: " + key + "with value: " + value)
    try:
        success_set =  r1.set(key, value)
        print ("Debug: Value of the return of set: %s",success_set)
        return (success_set)
    except:
        success_set = "failure not able to set the key and value"
        print (success_set)
        return (success_set)

def get_func(key):                                                 #Function to ge the value for the given key from the DB
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
        
def sync_function():
    print ("Debug: Inside sync function")


def replicate_func(port_repli):
    host = ''
    global update
    global data_1
    global time_1

    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to replica 2
    s1.bind((host, port_repli))
    print("Primary started on port: %s"%port_repli)
    s1.listen(1)
    print("Now Primary listening...\n")
    conn1, addr1 = s1.accept()                                              # Accept connection from replicas
    print ("Debug: Inside replicate_func")
    while True:
        print ('New connection from %s:%d' % (addr1[0], addr1[1]))
        data = conn1.recv(BUFSIZE)
        command = data.split()
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
            data_1 [key] = value
            time_1 [key] = time_id
            update = 1
            print ("Debug: printing dict value: ")
            print data_1
            print time_1
            success = set_func(key, value)
            print ("%s", success)
            #out_put1 = "Value: %s added for  key: %s" %(key, value) 
         #   out_put1 = "Value for key: " + key +  "is: " + value
         #   conn.send("success")



def non_primary_func(port_re):
    print ("Debug: Inside non_primary_func")
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect(('localhost',port_re))
    while True:
        cmd = raw_input('Enter a command: ')
        time_id =  str (int (time.time()) )
        print time_id
        cmd = cmd + " " + time_id
        print cmd
        conn.send(cmd)
        data = conn.recv(BUFSIZE)
        msglen = len(data)
        print "got: %s " % data
        print "received: %d" % msglen
        if data == '\0':
            print 'exiting...'
            sys.exit(0)


port1 = int (sys.argv[1])
if port1 == 17281:
    print ("************ I am Primary Replica (1) ************")
    port1 = port1 + 6
#    for replica in [replica2]:     #, replica2, replica3, replica4]:
    print ("Esatablishing connection for replica: replica1")               # %s " %(replica1)) 
    thread = threading.Thread(target = replicate_func, args = (port1, ))
    thread.start()
    #port1 = port1 + 1

else:
    port1 = port1 + 5 
    print ("************ I am Non-Primary Replica (%d) ************" % (port1))
#    for replica in [replica2]:     #, replica2, replica3, replica4]:
    print ("Esatablishing connection to Primary replica with port number: %d" % (port1)) 
    thread_c = threading.Thread(target = non_primary_func, args = (port1, ))
    thread_c.start()















host = ''               
port = int (sys.argv[1])                                                        #Port number is my lucky number :-)
BUFSIZE = 1024                                                                  #Max size handling per line of data from the client
#consistency = sys.argv[2]
r1 = pickledb.load("replica", False) 

print ("Running at port number:%d to communicate to client" %(port))

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                # Providing required deatils for the socket function to connect to client
s.bind((host, port))
print("Server started on port: %s"%port)
s.listen(1)                                 
print("Now listening...\n")
data_1 = { }
time_1 = { }

conn, addr = s.accept()                                              # Accept connection from the client
while True:
    print 'New connection from %s:%d' % (addr[0], addr[1])
    data = conn.recv(BUFSIZE)
    command = data.split()
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
            data_1 [key] = value
            time_1 [key] = time_id
            update = 1
            print ("Debug: printing dict value: ")
            print data_1
            print time_1
            success = set_func(key, value)
      #      print ("%s", success)
            #out_put1 = "Value: %s added for  key: %s" %(key, value) 
            out_put1 = "Value for key: " + key +  "is: " + value
            conn.send("success")

        elif command[0] == 'get':                              #Handling get command getting key value and displaying back to the client
            key = command[1]
            value = get_func(key)
            out_put1 = "Value for key: " + key +  "is: " + value
            conn.send(out_put1)
        
        else:
            out_put1 = "Not a proper command supported commands are set and get in this DB..."
            print out_put1
            conn.send(out_put1)

#        p = subprocess.Popen(data, shell=True, stdout=subprocess.PIPE)
#        conn.send(''.join([line for line in p.stdout.xreadlines()]))
#        conn.send(data)
     

thread.join()
thread_c.join()
print "thread finished...exiting"

def quit(conn):
    conn.close()
quit(conn)
