from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import os
import json

class Proj_Part_D_3_1(MRJob):

	def mapper_1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				from_address = str(fields[1])
				to_address = str(fields[2])
				value = float(fields[3])
				timef = int(fields[6])
				#hour_val = int(time.strftime("%H", time.gmtime(timef))) #extract hour			
				day_val = int(time.strftime("%j", time.gmtime(timef))) #extract day of the year
				year_val = int(time.strftime("%y", time.gmtime(timef))) #extract year
				if value > 0:
					from_key = (from_address, year_val, day_val)
					from_data = (value, 1)					
					to_key = (to_address, year_val, day_val)
					to_data = (value, 2)
					yield (from_key, from_data)
					yield (to_key, to_data)
		except:
			pass

	def reducer_1(self,word,counts):


		sent_value = 0.0
		received_value = 0.0

		try:
			for each in counts:

				if each[1] == 1 :
					sent_value += each[0]
				elif each[1] == 2 :
					received_value += each[0]

			if sent_value and received_value and received_value == sent_value :

				yield(word[0],received_value)

		except:
			pass

	def mapper_2(self,word,counts):	

		yield(word,counts)

	def reducer_2(self,word,counts):	

		yield(word,sum(counts))

	def combiner_2(self,word,counts):	

		yield(word,sum(counts))

	def mapper_3(self,word,counts):

		yield(None,(word,counts))

	def reducer_3(self, _, counts):
		sorted_temp = sorted(counts, reverse = True, key = lambda x: x[1])

		for each in sorted_temp:
			yield(each[0], each[1])


	def steps(self):

		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2, combiner = self.combiner_2), MRStep(mapper = self.mapper_3, reducer=self.reducer_3)]



if __name__ == '__main__':

	Proj_Part_D_3_1.run()

