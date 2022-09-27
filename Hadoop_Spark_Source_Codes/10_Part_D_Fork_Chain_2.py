from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time

class Proj_Part_D5_1_1(MRJob):
	
	def mapper1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				value = float(fields[3])
				if value > 0:
					timef = int(fields[6])
					month_val = time.strftime("%m", time.gmtime(timef))   # extract month
					year_val = time.strftime("%Y", time.gmtime(timef)) #extract year
					if int(year_val)==2017 and int(month_val)== 10:
						to_address = str(fields[2])
						rec_value = value
						from_address = str(fields[1])
						sent_value = value*(-1)
						yield (to_address, rec_value)
						yield (from_address, sent_value)

		except:
			pass

	def reducer1(self,word,counts):
		yield(word,sum(counts))

	def combiner1(self,word,counts):
		yield(word,sum(counts))

	def mapper2(self, key,counts):

		yield (None, (key,counts))


	def reducer2(self, _, counts):

		sorted_temp = sorted(counts, reverse = True, key = lambda x: x[1])

		for i in range(0,10):
			yield(sorted_temp[i][0], sorted_temp[i][1])


	def combiner2(self, _, counts):

		sorted_temp = sorted(counts, reverse = True, key = lambda x: x[1])

		for i in range(0,10):
			yield(None, sorted_temp[i])


	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1 , combiner = self.combiner1), MRStep(mapper = self.mapper2, reducer = self.reducer2, combiner = self.combiner2)]




if __name__ == '__main__':

	Proj_Part_D5_1_1.run()
