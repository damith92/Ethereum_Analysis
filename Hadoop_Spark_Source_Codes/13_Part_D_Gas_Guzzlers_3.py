from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import os
import json

class Proj_Part_D_2_3(MRJob):

	top_ten = []

	def mapper_init_1(self):
		# load top 10 srvices
		with open("partB_1.txt") as f: 
			for line in f:
				fields = line.split("\t")
				if len(fields) == 2 :
					self.top_ten.append(str(fields[0]).strip("\""))


	
	def mapper1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				to_address = str(fields[2])
				if to_address in self.top_ten[:3]:
					block = float(fields[0])
					gas_price = float(fields[5])
					timef = int(fields[6])
					month_val = time.strftime("%m", time.gmtime(timef)) # extract month
					year_val = time.strftime("%y", time.gmtime(timef)) #extract year
					time_key = (year_val, month_val)
					value_tuple = (0, gas_price, time_key)
					yield (block, value_tuple)

			elif len(fields) == 9:
				block = float(fields[0])
				difficulty = float(fields[3])
				timef = int(fields[7])
				month_val = time.strftime("%m", time.gmtime(timef)) # extract month
				year_val = time.strftime("%y", time.gmtime(timef)) #extract year
				time_key = (year_val, month_val)
				value_tuple = (1, difficulty, time_key)
				yield (block, value_tuple)


		except:
			pass

	def reducer1(self,word,counts):

		dff_list = []
		gas_list = []
		cont_flg = False

		for each in counts:

			if each[0]==0 :
				gas_list.append((each[1],each[2]))
				cont_flg = True
			elif each[0]==1:
				dff_list.append((each[1],each[2]))
			
				

		if cont_flg:
			if gas_list :
				for each in gas_list:
					yield (each[1],(0, each[0]))
			if dff_list :
				for each in dff_list:
					yield (each[1],(1, each[0]))

	def mapper2(self, key,values):
		yield (key, (values[0], values[1], 1))

	def reducer2(self,word,counts):
		try:
			itc1 = 0
			itc2 = 0
			total_diff = 0.0
			total_gas = 0.0
			for each in counts :
				if each[0] == 0:
					total_gas += each[1]				
					itc1 += each[2]
				elif each[0] == 1:
					total_diff += each[1]				
					itc2 += each[2]

			avg_diff = float(total_diff)/ float(itc2)
			avg_gas = float(total_gas)/ float(itc1)

			yield(word,(avg_gas, avg_diff))

		except:
			pass	

	def combiner2(self,word,counts):

		try:
	
			itc1 = 0
			itc2 = 0
			total_diff = 0.0
			total_gas = 0.0
			for each in counts :
				if each[0] == 0:
					total_gas += each[1]				
					itc1 += each[2]
				elif each[0] == 1:
					total_diff += each[1]				
					itc2 += each[2]

			interm_vals_gas = (0, total_gas, itc1)
			interm_vals_diff = (1, total_diff, itc2)
			yield(word,interm_vals_gas)
			yield(word,interm_vals_diff)
		except:
			pass


	def steps(self):
		return [MRStep(mapper_init=self.mapper_init_1, mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, combiner = self.combiner2, reducer = self.reducer2)]


if __name__ == '__main__':

	Proj_Part_D_2_3.run()
