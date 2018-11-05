# MapReduce Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
# Connect SSH IP Port=2222 (Using putty)
# Switch to root : sudo -i
#yum install nano [editor] optional
#yum install python-pip
#pip install google-api-python-client==1.6.4
#pip install mrjob==0.5.11
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
# Add your  data to Hadoop HDFS
#MapReduce1: wget http://mohamedsamirakha.info/cisc432/mapReduceDemo1.py
#Run on hadoop
#test if it is working locally: python mapReduceDemo1.py ./mapReduceData.dat
#python mapReduceDemo1.py  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -r hadoop hdfs:///user/maria_dev/inputMapReduce/mapReduceData.dat
from mrjob.job import MRJob 
from mrjob.step import MRStep
from datetime import datetime
import sys


# "The map-reduce job here counts the number of reviews per each movie"
class MoviesReviewsCount(MRJob):
    def steps(self):
           return [ 
                MRStep(mapper=self.mapper_get_movies,	
                     	reducer=self.reducer_count_avg_reviews),
		MRStep(reducer=self.reducer_sort_reviews)
                  ]
    #Mapping Function
    def mapper_get_movies(self, _, line):
         (userId, movieId, rating, timestamp) = line.split('\t')
         yield movieId, rating

    #Reducer used to calculate total reviews and average
    def reducer_count_avg_reviews(self, key, values):
    count = 0
	total = 0
	for value in values:
		total += int(value)
		count += 1	
	avg = total/float(count)
	res =  str(count) + "," + key
        yield float(avg), (int(key),int(count))
	
    # Reducer used to sort
    def reducer_sort_reviews(self, avg, key):
	    for movie_info in (key):
		    yield None, ('%.02f'%float(avg), movie_info[0], movie_info[1])

if __name__ == '__main__':
    sys.stderr.write("starting your first MapReduce job \n")
    start_time = datetime.now()
    MoviesReviewsCount.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
	#Print the time diff in seconds.
    sys.stderr.write("Total execution time "+str(elapsed_time.seconds)+" Seconds\n")
