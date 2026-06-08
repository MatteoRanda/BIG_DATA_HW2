from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import numpy as np


################
#     QUESTIONS
################
# 1) the phi must be the same for both algorithm?
# 2) are there any value of hyperparameters we can tend to
# 3) why we include the element x in S only if the random number is <= p?
# 4) If I already know that an element appears n*(phi-epsilon times), do I still have to count its frequencies?
# 5) Are C and w the same value?
# 5) For each row of the hash table, the hash function is characterized by its own pair of a,b values?

################
#    THINKGS TO CHECK BETTER
################
# 1) where to put variables a, b, p, C: in the hash table
# 2) starting values of delta, epsilon



N = -1 # To be set via command line

def StickySampling(time, batch):
    """f(x) is the frequency of the item.
    The check to add a frequent item in the output is done in the if to avoid a second fro cycle.
    However, since we have to compute the frquency, we still count the frequency.
    """
    global N, PHI, DELTA, EPSILON 
    R = ln(1 / (PHI*DELTA)) / EPSILON
    P = R/N #sampling rate

    output = {} #final set to return with the frequent values and their frequencies
    for x in batch:
#       if element in S:
#           if f(x) >= n * (phi - epsilon):
#               if x not in output: #we already know that the element is frequent
#                   add (x,f(x)) to output
#           f(x) += 1
#       else:
#           if np.random.uniform(low=0.0, high=1.0) < p:
#               add (x,1) to S
#   return output


class HashTable:
    def __init__(self, w, a, b):
        #define the hash table
        self.table = [0] * w
        self.w = w
        self.a = a
        self.b = b

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
        p = 8191
        return ((self.a * key + self.b) % p ) % self.w

    def __insert__(self, key):
        index = self._hash(key) #find the index

        self.table[index] += 1 #increase of 1 the value.
        # Recall that in CountMinSketch algorithm, the collision in the hash table is not handled as ususal
        # creating a linked list, but we just increase of 1 the value

    def getindex(self, key):
        #find the hash index for a given key in input
        index = self._hash(key)
        return index

    def find(self, key):


def CountMinSketch(time, batch, N, delta, phi, epsilon, a, b, C=0, p=8191):
    """
    d is the number of rows of the hash tables.
    Idea: create a object of HashTable class for each row;
    each hash function is characterized by a,b values, so we need to store them in pair I think
    C = w?
    some starting values: epsilon=0.001, delta=0.01
    """
    d = np.log(1/delta) #number rows
    w = 2/epsilon #number of columns
    min_freq = N * (phi-epsilon) #minimum frequence to be a frequent item

    item_freq = {} #dict of key-value pairs item : freq

    for x in batch:
        for j in range(d):
            hash_table[j].insert(x) #update the frequencies
            if x not in item_freq.keys():
                if hash_table[j] >= min_freq:
                    item_freq[x] = hash_table[j]
            if x in item_freq.keys():
                if hash_table[j] < item_freq[x]:
                    item_freq[x] = hash_table[j]

    return item_freq



if __name__ =="__main__":
    assert len(sys.argv) == 8, 'USAGE: port, n, phi, epsilon, delta, d, w, portExp'

    # These following lines are copied from the DistinctExample script
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")

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

    streamLength = [0]
    hash_table = [] 
    ab_memory = [] #to save the pairs of (a,b)
    for j in range(d):
        a = np.random.randint(0,p-1)
        b = np.random.randint(1,p-1)
        ab_memory.append((a,b))

        hash_table.append(HashTable(w=W, a=a, b=b))


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
    for item in true_frequent_items.keys(): 
        print(f'Item = {item} True Freq = {true_frequent_item[item]}')
    print('STICKY SAMPLING')
    for item in sticky_sampling.keys():
        print(f'Item = {item} True Freq = {sticky_sampling[item]}')
    print('COUNT-MIN SKETCH')
    for item in count_min_sketch.keys():
        print(f'Item = {item} True Freq = {count_min_sketch[item]}')
