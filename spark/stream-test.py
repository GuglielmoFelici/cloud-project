
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext


def stream():
    """
    Processes sample food establishment inspection data and queries the data to find the top 10 establishments
    with the most Red violations from 2006 to 2020.

    :param data_source: The URI of your food establishment data CSV, such as 's3://DOC-EXAMPLE-BUCKET/food-establishment-data.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/restaurant_violation_results'.
    """
    # with SparkSession.builder.appName("Stream data").getOrCreate() as spark:
    sc = SparkContext("local[2]", "NetworkWordCount")
    '''
    ssc = StreamingContext(sc, 4)
    # csvDF = ssc.textFileStream("s3://data-lake-test-guglielmo")
    csvDF = ssc.textFileStream(
        "file:////home/guglielmo/tmp/spark-stream-test/")
    csvDF.pprint()
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    '''
    ssc = StreamingContext(sc, 1)
    lines = ssc.textFileStream(
        "s3://test-guglielmo-1/*")
    lines.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    stream()
