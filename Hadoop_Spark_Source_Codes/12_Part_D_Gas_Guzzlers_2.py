from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import os
import json

class Proj_Part_D_2_2(MRJob):

	
	def mapper1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 9:
				blk_number = float(fields[0])
				difficulty = float(fields[3])
				gas_used = float(fields[6])
				timef = int(fields[7])
				month_val = time.strftime("%m", time.gmtime(timef))   # extract month
				year_val = time.strftime("%Y", time.gmtime(timef)) #extract year
				time_key = (year_val, month_val)
				value_tuple = (0, time_key, gas_used, difficulty)
				yield (blk_number, value_tuple)

			elif len(fields) == 5:
				blk_number = float(fields[3])
				yield (blk_number, (1,"In"))
		except:
			pass

	def reducer1(self,word,counts):

		vl_list = []
		cont_flg = False

		for each in counts:

			if each[0]==0 :
				vl_list.append((each[1],each[2], each[3]))
			elif each[0]==1:
				cont_flg = True
				

		if cont_flg:
			if vl_list :
				for each in vl_list:
					yield (each[0],(each[1],each[2]))

	def mapper2(self, key,values):
		yield (key, (values[0], values[1], 1))

	def reducer2(self,word,counts):

		itc = 0
		total_dif = 0
		total_gas = 0
		for each in counts :
			total_gas += each[0]
			total_dif += each[1]
			itc += each[2]

		avg_dif = float(total_dif)/ float(itc)
		avg_gas = float(total_gas)/ float(itc)

		yield(word,(avg_gas, avg_dif))

										


	def combiner2(self,word,counts):
	
		itc = 0
		total_dif = 0
		total_gas = 0
		for each in counts :
			total_gas += each[0]
			total_dif += each[1]
			itc += each[2]

		interm_vals = (total_gas, total_dif, itc)

		yield(word,interm_vals)

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, combiner = self.combiner2, reducer = self.reducer2)]




if __name__ == '__main__':

	Proj_Part_D_2_2.run()
