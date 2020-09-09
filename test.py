from pyspark import SparkContext

logFile = "abc.txt"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numGs = logData.filter(lambda s: 'g' in s).count()
print("A: " + str(numAs))
print("G: " + str(numGs))

