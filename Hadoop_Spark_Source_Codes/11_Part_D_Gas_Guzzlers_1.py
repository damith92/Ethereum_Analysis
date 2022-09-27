from mrjob.job import MRJob
import re
import time
import os
import json

class Proj_Part_D_2_1(MRJob):

	
	def mapper(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				gas_price = float(fields[5])
				timef = int(fields[6])
				month_val = time.strftime("%m", time.gmtime(timef)) # extract month
				year_val = time.strftime("%y", time.gmtime(timef)) #extract year
				time_key = (year_val, month_val)
				value_pair = (gas_price, 1)
				yield (time_key, value_pair)
		except:
			pass

	def reducer(self,word,counts):

		itc = 0
		total_val = 0
		for each in counts :
			total_val += each[0]
			itc += each[1]

		avg_vals = float(total_val)/ float(itc)

		yield(word,avg_vals)


	def combiner(self,word,counts):
	
		itc = 0
		total_val = 0
		for each in counts :
			total_val += each[0]
			itc += each[1]

		interm_vals = (total_val, itc)

		yield(word,interm_vals)




if __name__ == '__main__':

	Proj_Part_D_2_1.run()
