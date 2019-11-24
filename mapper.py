#!/usr/bin/env python
"""mapper.py"""

import sys
import numpy as np
import pydoop.hdfs as hdfs


def read_inputfile(filename):
    f=hdfs.open(filename, 'rt')
    data=f.read()
    return data
    

def data_formatting(data):
    values_array=[]
    id_array=[]
    
    for lines in data:
        #here id = cid and datarecord id and values is centroid values and data values
        id,values=lines.split("\t")
        #_x and _y centroids and data values x and y coordinates
        _x,_y=values.split(",")
        values_array.append([float(_x),float(_y)])
        id_array.append(int(id))
    return np.array(id_array),np.array(values_array)
    
def mapper(x,y):
    #read centroid file
    centroid_data = read_inputfile("Kmeans_python/input/centroids")
    cid_array,centroid_array=data_formatting(centroid_data.split("\r\n"))
    #print(cid_array,centroid_array)
    #print("data",[x,y])
    
    distance_calculation(centroid_array,[x,y])

#calcualting euclidean distance
def distance_calculation(array1,array2):
       #caluclating eucliden distance of every data record with all centroids and taking the centroid id with minimum distance
        distances=np.linalg.norm(array2-array1,axis=1)
        #assigning cluster label to each recored based on the euclidean distance
        clusterid=np.argmin(distances)
        #write to standard output in expected format which will be passed to reducer to write clutser label and associaated data file after mapreduce
        print(clusterid,"\t",",".join(list(map(str,array2))))
        
if __name__ == "__main__":
    #read datafile
    for data in sys.stdin:
        #split data.hdfs
        recordvalue_x,recordvalue_y=data.split(",")
        mapper(float(recordvalue_x),float(recordvalue_y))



