from mrjob.job import MRJob
from mrjob.step import MRStep

class Proj_Part_B(MRJob):

	

	def mapper_1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				address_to = str(fields[2])
				value = float(fields[3])
				
				yield (address_to, (value,1))

			elif len(fields) == 5:
				address = str(fields[0])
				yield (address, ("In",2))
		except:
			pass


	def reducer_1(self, key, counts):

		contract_flag = 0
		values_list = []
		for each in counts:
			if each[1]==1:
				values_list.append(each[0])
			elif each[1] == 2:
				contract_flag = 1

		if contract_flag == 1:
			yield (key, sum(values_list))

	def mapper_2(self, key,counts):

		yield (None, (key,counts))


	def reducer_2(self, _, counts):

		sorted_temp = sorted(counts, reverse = True, key = lambda x: x[1])

		for i in range(0,10):
			yield(sorted_temp[i][0], sorted_temp[i][1])


	def combiner_2(self, _, counts):

		sorted_temp = sorted(counts, reverse = True, key = lambda x: x[1])

		for i in range(0,10):
			yield(None, sorted_temp[i])


	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2, combiner = self.combiner_2)]

if __name__ == '__main__':
	Proj_Part_B.JOBCONF= { 'mapreduce.job.reduces': '4' }
	Proj_Part_B.run()
