from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys

################
#     QUESTIONS
################
# 1) the phi must be the same for both algorithm?
# 2) are there any value of hyperparameters we can tend to
# 3) why we include the element x in S only if the random number is <= p?



N = -1 # To be set via command line

#def StickySampling(stream):
"""f(x) is the frequency of the item. 
The check to add a frequent item in the output is done in the if to avoid a second fro cycle.
However, since we have to compute the frquency, we still count the frequency. """
#   define hyperparameters: delta in (0,1), phi in (0,1) and epsilon in (0,phi) 
#   delta, phi = 
#   epsilon = 
#  
#   n = len(stream)
#   r = ln(1 / (phi*delta)) / epsilon 
#   p = r/n #sampling rate
#
#   output = {} #final set to return with the frequent values and their frequencies 
#   for x in stream:
#       if element in S:
#           if f(x) >= n * (phi - epsilon):
#               if x not in output: #we already know that the element is frequent
#                   add (x,f(x)) to output
#           f(x) += 1
#       else:
#           if np.random.rand(0,1) < p: 
#               add (x,1) to S
#   return output


#def CountMinSketch(): 




if __name__ =="__main__":
    assert len(sys.argv) == 8, 'USAGE: port, n, phi, epsilon, delta, d, w, portExp'

    # These following lines are copied from the DistinctExample script
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")

    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.1)  # Batch duration of 0.1 sec = 100 ms
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event() # semafor


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

    streamLength = [0]
    histogram = {} #hash table for the Count-Min Sketch

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
    print("Number of items processed =", streamLength[0])
    print("Number of distinct items =", len(histogram))
    largest_item = max(histogram.keys())
    print("Largest item =", largest_item)
