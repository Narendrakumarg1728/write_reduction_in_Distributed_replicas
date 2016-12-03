#Client program 

import sys, socket, time, random

port = int (sys.argv[1])
BUFSIZE = 1024
conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect(('localhost',port))
while True:
#    cmd = raw_input('Enter a command: '
    key = random.randint(1 ,10)
    if port == 17201:
        value = key * 1
    elif port == 17301:
        value = key * 10
    elif port == 17401:
        value = key * 100
    elif port == 17501:
        value = key * 1000

    cmd = "set "  + str(key) + " " + str(value)
    print cmd
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
