from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys


N = -1 # To be set via command line

#def StickySampling():
 



#def CountMinSketch(): 




if __name__ =="__main__":
    assert len(sys.argv) == 7, 'USAGE: port, n, phi, epsilon, delta, d, w, portExp'

    # These following lines are copied from the DistinctExample script
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")

    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.1)  # Batch duration of 0.1 sec = 100 ms
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()


    N = int(sys.argv[1])
    print(f'N = {N}')
    PHI = int(sys.argv[2]) #frequency threshold in range (0,1)
    print(f'Frequency Threshold = {N}')
    EPSILON = float(sys.argv[3]) #accuracy parameter in range (0, PHI)
    print(f'Accuracy parameter = {N}')
    DELTA = float(sys.argv[4]) #confidence parameter in (0,1)
    print(f'Confidence parameter = {N}')
    D = int(sys.argv[5]) #number of rows  in count-min sketch
    print(f'Number of rows in the count-min sketch = {N}')
    W = int(sys.argv[6]) #number of columns in count-min sketch
    print(f'Number of columns in the count-min sketch = {N}')
    PORTEXP = int(sys.argv[7]) #port number
    print(f'Receiving data from port = {PORTEXP}')

