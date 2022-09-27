from mrjob.job import MRJob
import re
import time

class Proj_Part_A1(MRJob):
	
	def mapper(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				timef = int(fields[6])
				month_val = time.strftime("%m", time.gmtime(timef)) # extract month
				year_val = time.strftime("%y", time.gmtime(timef)) #extract year
				time_key = (year_val, month_val)
				yield (time_key, 1)
		except:
			pass

	def reducer(self,word,counts):
		yield(word,sum(counts))

	def combiner(self, word, counts):
		yield(word, sum(counts))

if __name__ == '__main__':
	Proj_Part_A1.JOBCONF= { 'mapreduce.job.reduces': '4' }
	Proj_Part_A1.run()
