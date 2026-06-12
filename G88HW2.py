from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import numpy as np


################
#     QUESTIONS
################


################
#    THINKGS TO CHECK BETTER
################


################
#    MISSING
################


N = -1 # To be set via command line

class HashTable:
    def __init__(self, w, a, b, p):
        #define the hash table
        self.table = [0] * w
        self.w = w
        self.a = a
        self.b = b
        self.p = p

    def __len__(self):
        return len(self.table)

    def __contains__(self, key, default = None):
        #if missing key, return default value
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key):
        #find the hash value for a given key in input
        index = self._hash(key)
        return self.table[index]

    def _hash(self, key):
        #given a key, it returns an index for the key-value pair
        return ((self.a * key + self.b) % self.p ) % self.w

    def insert(self, key, frequency):
        index = self._hash(key) #find the index
        self.table[index] += frequency #increase the value.
        # Recall that in CountMinSketch algorithm, the collision in the hash table is not handled as ususal
        # creating a linked list, but we just increase of 1 the value

    def getindex(self, key):
        #find the hash index for a given key in input
        index = self._hash(key)
        return index

#################### MUST BE CHANGED
def ExactCounting(x, c):
    global exact_frequency
    if x in exact_frequency.keys():
        exact_frequency[x] += c
    else:
        exact_frequency[x] = c
    

def StickySampling(x, c):
    """f(x) is the frequency of the item.
    The check to add a frequent item in the output is done in the if to avoid a second for cycle.
    However, since we have to compute the frquency, we still count the frequency.
    """
    global N, PHI, DELTA, EPSILON, histogram
    R = np.log(1 / (PHI*DELTA)) / EPSILON
    pr = R/N #sampling rate

    if x in histogram:
        histogram[x] += c
    else:
        if np.random.uniform(low = 0.0, high=1.0) < pr:
            histogram[x] = c
    


def CountMinSketch(x, c):
    """
    d is the number of rows of the hash tables.
    Idea: create a object of HashTable class for each row;
    each hash function is characterized by a,b values, so we need to store them in pair I think
    some starting values: epsilon=0.001, delta=0.01
    """
    global N, PHI, D, W, count_min_sketch, output_countmin, min_freq
    #count_min_sketch is an hash table with D rows and W columns
    minimum = float('inf')

    for j in range(D):
        count_min_sketch[j].insert(x, c)

        if count_min_sketch[j][x] < minimum:
            minimum = count_min_sketch[j][x]

    if minimum >= min_freq:
        output_countmin[x] = minimum                


def Container(time, batch):

    global count_min_sketch, output_countmin, true_frequent_items
    global histogram, exact_frequency 
    global N, PHI, DELTA, EPSILON, D, W, min_freq 
    #count_min_sketch is an hash table with D rows and W columns

    batch_size = batch.count()
    # If we already have enough points (> N), skip this batch.
    if StreamLength[0]>=N:
        return
    StreamLength[0] += batch_size

    item_freq = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda a, b: a+b).collectAsMap() # collectAsMap() returns the RDD as a dictionary 
    # collectAsMap() returns the RDD as a dictionary 
    #we created a dictionary with key = item, value = frequency of the item
    for x, c in item_freq.items():

        ExactCounting(x, c)
        StickySampling(x, c)
        CountMinSketch(x, c)
    
    
    if StreamLength[0] >= N:
        stopping_condition.set()

if __name__ =="__main__":
    assert len(sys.argv) == 8, 'USAGE: port, n, phi, epsilon, delta, d, w, portExp'

    # These following lines are copied from the DistinctExample script
    conf = SparkConf().setMaster("local[*]").setAppName("G88HW2")

    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.1)  # Batch duration of 0.1 sec = 100 ms
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event() # semafor


    N = int(sys.argv[1]) #maximum number of elements that can be processed
    print(f'n = {N}')
    PHI = float(sys.argv[2]) #frequency threshold in range (0,1)
    print(f'phi = {PHI}')
    EPSILON = float(sys.argv[3]) #accuracy parameter in range (0, PHI)
    print(f'epsilon = {EPSILON}')
    DELTA = float(sys.argv[4]) #confidence parameter in (0,1)
    print(f'delta = {DELTA}')
    D = int(sys.argv[5]) #number of rows  in count-min sketch
    print(f'd = {D}')
    W = int(sys.argv[6]) #number of columns in count-min sketch
    print(f'w = {W}')
    PORTEXP = int(sys.argv[7]) #port number
    print(f'port = {PORTEXP}')


    StreamLength = [0]
    exact_frequency = {}
    true_frequent_items = {}

    histogram = {}
    sticky_sampling = {} #final set of Sticky sampling with the frequent values and their frequencies
    
    output_countmin = {} #to print
    
    min_freq = N * PHI
    P=8191
    items_freq = {} #dict of key-value pairs item : freq for count-min sketch
    count_min_sketch = [] 
    ab_memory = [] #to save the pairs of (a,b)
    for j in range(D):
        a = np.random.randint(1,P-1)
        b = np.random.randint(0,P-1)
        ab_memory.append((a,b))

        count_min_sketch.append(HashTable(w=W, a=a, b=b, p=P))


    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", PORTEXP, StorageLevel.MEMORY_AND_DISK)

    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: Container(time, batch))    




    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")

    # The following command stops the execution of the stream. The first boolean, if true, also
    # stops the SparkContext, while the second boolean, if true, stops gracefully by waiting for
    # the processing of all received data to be completed. You might get some error messages when the
    # program ends, but they will not affect the correctness.

    ssc.stop(False, False)
    print("Streaming engine stopped")

    sticky_sampling = {x : freq for x, freq in histogram.items() if freq >= min_freq}
    true_frequent_items = {x : freq for x, freq in exact_frequency.items() if freq >= min_freq}

    # COMPUTE AND PRINT FINAL STATISTICS
    print('TRUE FREQUENT ITEMS')
    for item in true_frequent_items.keys(): 
        print(f'Item = {item} True Freq = {true_frequent_items[item]}')
    print('STICKY SAMPLING')
    for item in sticky_sampling.keys():
        print(f'Item = {item} True Freq = {sticky_sampling[item]}')
    print('COUNT-MIN SKETCH')
    for item in output_countmin.keys():
        print(f'Item = {item} True Freq = {output_countmin[item]}')
