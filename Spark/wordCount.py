from pyspark import SparkContext
logFilepath = "file:///Users/saireddyavs/Desktop/airflow-dags/Spark/Resources/Sample.txt"
sc = SparkContext("local", "first app")
text_file = sc.textFile(logFilepath).cache()


counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda a: -a[1])

output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

sc.stop()
