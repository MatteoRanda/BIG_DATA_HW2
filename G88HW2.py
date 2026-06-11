from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import numpy as np


################
#     QUESTIONS
################
# 1) the phi must be the same for both algorithm? Yes
# 2) are there any value of hyperparameters we can tend to? Given by prof

################
#    THINKGS TO CHECK BETTER
################
# 1) where to put variables a, b, p, C: in the hash table
# 2) starting values of delta, epsilon
# 3) how to return the value in count-min sketch


################
#    MISSING
################
# 1) compute exact frequency



N = -1 # To be set via command line

def Exact_Counting(time, batch):
    global true_frequent_items, N, PHI
    frequency = {}
    for x in batch:
        if x in frequency.keys():
            frequency[x] += 1
            if x in true_frequent_items.keys():
                true_frequent_items[x] += 1
            else:
                if frequency[x] >= N * PHI:
                    true_frequent_items[x] = 1
        else:
            frequency[x] = 1


def StickySampling(time, batch):
    """f(x) is the frequency of the item.
    The check to add a frequent item in the output is done in the if to avoid a second fro cycle.
    However, since we have to compute the frquency, we still count the frequency.
    """
    global N, PHI, DELTA, EPSILON, histogram
    R = np.log(1 / (PHI*DELTA)) / EPSILON
    p = R/N #sampling rate
    min_freq = N*PHI

    # use at the end: .filter(lambda x: x[1]> N*PHI).collectAsMap()
    # filter return only the elements satisfying the condition; 

    item_freq = batch.map(lambda s: (int(s), 1))
    .reduceByKey(lambda a, b: a+b)
    .collectAsMap() # collectAsMap() returns the RDD as a dictionary 
    #we created a dictionary with key = item, value = frequency of the item


    for x, c in item_freq.items(): 
        if x in histogram:
            histogram[x] += c
        else:
            if np.random.uniform(low = 0.0, high=1.0) < p:
            histogram[x] = c
    
    sticky_sampling = {x : freq for x, freq in histogram.items() if freq >= min_freq}

            


    for x in batch_items: #process stream
        if x in sticky_sampling.keys():
            #if the element is already a frequent item, we don't need to touch the histogram, but we increase directly the solution
            sticky_sampling[x] += 1 
        else:
            if x in histogram: #if x is in the hash_table, we can increase frequence
                histogram[x] += 1
                if histogram[x] >= PHI*N:
                    sticky_sampling[x] = histogram[x] 
            else: #if not, we randomly decide to put it in histogram
                if np.random.uniform(low = 0.0, high=1.0) < p:
                    histogram[x] = 1


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

    def __contain__(self, key, default = None):
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


def CountMinSketch(time, batch):
    """
    d is the number of rows of the hash tables.
    Idea: create a object of HashTable class for each row;
    each hash function is characterized by a,b values, so we need to store them in pair I think
    C = w?
    some starting values: epsilon=0.001, delta=0.01
    """
    global N, PHI, D, W, count_min_sketch, output_countmin
    #count_min_sketch is an hash table with D rows and W columns
    min_freq = N * PHI #minimum frequence to be a frequent item

    #we can remove this from the algoirhtm and compute it once outside the functions
    item_freq = batch.map(lambda s: (int(s), 1))
    .reduceByKey(lambda a, b: a+b)
    .collectAsMap() # collectAsMap() returns the RDD as a dictionary 
    #we created a dictionary with key = item, value = frequency of the item

    for x, c in item_freq.items():
        minimum = float('inf')
        for j in range(D):
            count_min_sketch[j].insert(x, c)

            if count_min_sketch[j][x] < minimum:
                minimum = count_min_sketch[j][x]

        if minimum >= min_freq:
            output_countmin[x] = minimum                



if __name__ =="__main__":
    assert len(sys.argv) == 8, 'USAGE: port, n, phi, epsilon, delta, d, w, portExp'

    # These following lines are copied from the DistinctExample script
    conf = SparkConf().setMaster("local[*]").setAppName("G88HW2")

    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.1)  # Batch duration of 0.1 sec = 100 ms
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event() # semafor


    N = int(sys.argv[1])
    print(f'n = {N}')
    PHI = int(sys.argv[2]) #frequency threshold in range (0,1)
    print(f'phi = {N}')
    EPSILON = float(sys.argv[3]) #accuracy parameter in range (0, PHI)
    print(f'epsilon = {N}')
    DELTA = float(sys.argv[4]) #confidence parameter in (0,1)
    print(f'delta = {N}')
    D = int(sys.argv[5]) #number of rows  in count-min sketch
    print(f'd = {N}')
    W = int(sys.argv[6]) #number of columns in count-min sketch
    print(f'w = {N}')
    PORTEXP = int(sys.argv[7]) #port number
    print(f'port = {PORTEXP}')

    true_frequent_item = {}
    sticky_sampling = {} #final set of Sticky sampling with the frequent values and their frequencies
    count_min_sketch = {}
    output_countmin = {} #to print
    

    P=8191
    items_freq = {} #dict of key-value pairs item : freq for count-min sketch
    hash_table = [] 
    ab_memory = [] #to save the pairs of (a,b)
    for j in range(d):
        a = np.random.randint(1,P-1)
        b = np.random.randint(0,P-1)
        ab_memory.append((a,b))

        hash_table.append(HashTable(w=W, a=a, b=b, p=P))


    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", PORTEXP, StorageLevel.MEMORY_AND_DISK)

    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: StickySampling(time, batch))



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

    # COMPUTE AND PRINT FINAL STATISTICS
    print('TRUE FREQUENT ITEMS')
    for item in true_frequent_item.keys(): 
        print(f'Item = {item} True Freq = {true_frequent_item[item]}')
    print('STICKY SAMPLING')
    for item in sticky_sampling.keys():
        print(f'Item = {item} True Freq = {sticky_sampling[item]}')
    print('COUNT-MIN SKETCH')
    for item in count_min_sketch.keys():
        print(f'Item = {item} True Freq = {count_min_sketch[item]}')
