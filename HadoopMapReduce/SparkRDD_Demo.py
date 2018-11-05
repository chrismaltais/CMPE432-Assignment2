# Spark RDD Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
#ambari-admin-password-reset [ in case you want to use ambari admin (username=admin, password=you need to add), instead of maria_dev
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
#Spark Demo: wget http://mohamedsamirakha.info/cisc432/SparkRDD_Demo.py
#Run on hadoop
#spark-submit ./SparkRDD_Demo.py

from pyspark import SparkConf, SparkContext
from datetime import datetime
import sys

# The spark map-reduce function here count the number of reviews per each movies
def readInputFile(line):
    data = line.split("\t")
    ## First field key
    return (int(data[1]), (int(data[2]), 1.0))

# val = (MovieID, (Rating, NumberOfReviews))
# val[0] = MovieID
# val[1][0] = Rating
# val[1][1] = NumberOfReviews
def get_avg(val):
    return(val[0], (val[1][0]/val[1][1], val[1][1]))

# val = (MovieID, (Rating, NumberOfReviews)
# val[1] = (Rating, NumberOfReviews)
# Therefore sorting based on rating first THEN  number of reviews (ascending)
def sorting(val):
    return(val[1])

if __name__ == "__main__":
    # Start Timer
    start_time = datetime.now()

    #  create   SparkContext
    conf = SparkConf().setAppName("Cisc432Spark")
    sc = SparkContext(conf = conf)

    # RDD fileLines: read data from HDFS, same as I did in Hadoop mapReduce Demo, you should add yours!
    fileLines = sc.textFile("hdfs:///user/assignment2/netIDs.dat")

    # RDD  movieRatings : Convert to (movieID, (rating, 1.0))
    movieRatings = fileLines.map(readInputFile)

    # a = Movie 1
    # b = Movie 2
    # a[0] = Rating for Movie 1
    # a[1] = Count increment for Movie 1
    # b[0] = Rating for Movie 2
    # b[1] = Count increment for Movie 2
    sumOfCountsandRating = movieRatings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avg_of_ratings = sumOfCountsandRating.map(get_avg)

    # RDD: Sort by sum
    sortedResults = avg_of_ratings.sortBy(sorting)

    # End Timer
    end_time = datetime.now()

    # Total Time to Execute SparkRDD Operations
    elapsed_time = end_time - start_time

    # Change this value to 1000 to verify output is sorted by rating
    numberOfResults = 20
    sortedResultsLimit10 = sortedResults.take(numberOfResults)

    # Print them out:
    print("Output format: (MovieID, (Rating, Number of Reviews))")
    for result in sortedResultsLimit10:
        print(result[0], result[1])

    # Print Total Time
    print("Total time to execute SparkRDD operations: "+ str(elapsed_time.seconds) + " seconds.")