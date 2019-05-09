from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    """
    # YOUR CODE HERE
    pos_count, neg_count = [], []

    for count in counts:
        if count:
            pos_count.append(count[0][1])
            neg_count.append(count[1][1])
    ax = plt.subplot(111)
    x = range(0, len(pos_count))
    # pdb.set_trace()
    ax.plot(x, pos_count, 'bo-', label="positive")
    ax.plot(x, neg_count, 'go-', label="negative")
    y_max = max(max(pos_count), max(neg_count)) + 50
    ax.set_ylim([0, y_max])
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    plt.legend(fontsize='small', loc=0)
    plt.savefig("plot.png")
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    word = []
    f = open(filename, 'r')
    for i in f:
        word.append(i.strip())
    return set(word)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)

    word1 = tweets.flatMap(lambda line: line.split(' '))
    word2 = word1.map(lambda str: ('positive', 1) if str in pwords else ('negative', 1) if str in nwords else ('none', 1))
    word3 = word2.filter(lambda x: x[0] == 'positive' or x[0] == 'negative').reduceByKey(lambda x, y: x + y)
    updatedWords = word3.updateStateByKey(updateFunction)
    updatedWords.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    word3.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts


if __name__=="__main__":
    main()
