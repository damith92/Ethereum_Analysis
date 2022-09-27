from mrjob.job import MRJob
from mrjob.step import MRStep

class Proj_Part_C(MRJob):

	

	def mapper_1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9:
				miner_id = str(fields[2])
				blk_size = float(fields[4])
				
				yield (miner_id, blk_size)

		except:
			pass


	def reducer_1(self, key, counts):

		yield (key, sum(counts))

	def combiner_1(self, key, counts):

		yield(key, sum(counts))


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
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1, combiner=self.combiner_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2, combiner = self.combiner_2)]

if __name__ == '__main__':
	#Proj_Part_C.JOBCONF= { 'mapreduce.job.reduces': '4' }
	Proj_Part_C.run()
