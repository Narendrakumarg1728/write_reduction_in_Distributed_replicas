import sys
import csv

filename = sys.argv[1]
#with open(filename, 'r') as f1:
#  reader = csv.reader(f1)
#  your_list = list(reader)

#print your_list

#size_of_list = len(your_list)

#print ("Number of writes =%d" % size_of_list)




f = open(filename, "r");
list_y = []
for line in f:
    entry  = line.strip().split(",")
    print (entry)
    writes = len (entry)
    print writes

writes = len (entry)
print ("Number of writes =%d" % writes)



