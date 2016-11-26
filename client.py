#Client program 

import sys, socket, time

port = int (sys.argv[1])
BUFSIZE = 1024
conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect(('localhost',port))
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
