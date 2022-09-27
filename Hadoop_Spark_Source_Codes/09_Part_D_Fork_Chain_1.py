from mrjob.job import MRJob
import re
import time

class Proj_Part_D5_1(MRJob):
	
	def mapper(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				timef = int(fields[6])
				day_val = int(time.strftime("%d", time.gmtime(timef))) #extract day of the month
				month_val = time.strftime("%m", time.gmtime(timef))   # extract month
				year_val = time.strftime("%Y", time.gmtime(timef)) #extract year
				time_key = day_val
				if int(year_val)==2017 and int(month_val)==10 :
					value_pair = (float(fields[5]), 1)
					yield (time_key, value_pair)
		except:
			pass

	def reducer(self,word,counts):
		itc = 0
		total_val = 0
		for each in counts :
			total_val += each[0]
			itc += each[1]
            
		avg_gas_pr = total_val/float(itc)
		yield(word,(total_val, itc, avg_gas_pr)


	def combiner(self,word,counts):
		itc = 0
		total_val = 0
		for each in counts :
			total_val += each[0]
			itc += each[1]

		interm_vals = (total_val, itc)

		yield(word,interm_vals)



if __name__ == '__main__':

	Proj_Part_D5_1.run()
