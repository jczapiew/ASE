from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession


url = "https://stackoverflow.com/questions/57014043/reading-data-from-url-using-spark-databricks-platform"

logFile = "url"
sc = SparkContext("", "first app")

words = sc.parallelize (
   ["caa",
    "1aa",
    "dfad"]
)
words_filter = words.filter(lambda x: 'aa' in x)
filtered = words_filter.collect()
print(filtered)

spark = SparkSession(sc)

input_bucket = 's3://commoncrawl/cc-index/table/cc-main/warc/'
df = spark.read.load(input_bucket)
df.createOrReplaceTempView('ccindex')
sqlDF = spark.sql('SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = "CC-MAIN-2020-16" AND subset = "warc" AND url_host_tld = "pl"')

sqlDF.show()

