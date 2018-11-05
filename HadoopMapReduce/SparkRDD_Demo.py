# Spark RDD Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
#ambari-admin-password-reset [ in case you want to use ambari admin (username=admin, password=you need to add), instead of maria_dev
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
#Spark Demo: wget http://mohamedsamirakha.info/cisc432/SparkRDD_Demo.py
#Run on hadoop
#spark-submit ./SparkRDD_Demo.py

from pyspark import SparkConf, SparkContext

# The spark map-reduce function here count the number of reviews per each movies
def readInputFile(line):
    data = line.split(",")
        ## First field key
    print "MovieID: ", int(data[1])
    return (int(data[1]), (int(data[2]), 1.0))

def get_avg(val):
        if val[0] == 1565:
                print "I FOUUUUUUUUUUUUUUUUUUUUUND 1565"
        print "Printing val[0]:", val[0]
        print "Printing val[1]:", val[1]
        print "Printing val:", val
        print "Printing val[1]/val[0]:", val[1][0]/val[1][1]
        return(val[0], (val[1][0]/val[1][1], val[1][1]))

def sorting(val):
        print(val)
        return(val[1])

if __name__ == "__main__":
    #  create   SparkContext
    conf = SparkConf().setAppName("Cisc432Spark")
    sc = SparkContext(conf = conf)

    # RDD fileLines: read data from HDFS, same as I did in Hadoop mapReduce Demo, you should add yours!
    fileLines = sc.textFile("hdfs:///user/maria_dev/inputMapReduce/mapReduceData.dat")

    # RDD  movieRatings : Convert to (movieID, (rating, 1.0))
    movieRatings = fileLines.map(readInputFile)

    # RDD: Reduce to (movieID, (sumOfRatings))
    #sumOfCountsandRating = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[1] + movie2[1]) )
    #sumOfCountsandRating = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1 + movie2) )

    # Key, (Count, Sum of Ratings)
    # a = Key
    # b[0] = (count aka 1.0)
    #b[1] = (ratings)
    sumOfCountsandRating = movieRatings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avg_of_ratings = sumOfCountsandRating.map(get_avg)

    # RDD: Sort by sum
    sortedResults = avg_of_ratings.sortBy(sorting)

    # Take the top 10 results
    sortedResultsLimit10 = sortedResults.take(1000)

    # Print them out:
    for result in sortedResultsLimit10:
        print(result[0], result[1])
