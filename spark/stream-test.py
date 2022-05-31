
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext


def stream():
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 4)
    csvDF = ssc.textFileStream(
        "file:////home/guglielmo/tmp/spark-stream-test/")
    csvDF.pprint()
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == "__main__":
    stream()
