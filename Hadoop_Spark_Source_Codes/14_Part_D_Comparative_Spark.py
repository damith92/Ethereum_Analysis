import pyspark
import time

def fl_transactions_fn(line):
	try:
		fields = line.split(',')
		if len(fields)==7:
			int(fields[3])
			return True
		else:
			return False

	except:
		return False

def fl_contracts_fn(line):
	try:
		fields = line.split(',')
		if len(fields)==5:
			return True
		else:
			return False

	except:
		return False

start_time = time.time()

sc = pyspark.SparkContext()

#Process transactions
transactions_ls = sc.textFile("/data/ethereum/transactions")
valid_transaction_ls = transactions_ls.filter(fl_transactions_fn)
map_transaction_ls = valid_transaction_ls.map(lambda x : (x.split(',')[2], int(x.split(',')[3])))
aggregate_transaction_ls = map_transaction_ls.reduceByKey(lambda k1, k2 : k1 + k2)

#Process contracts
contracts_ls = sc.textFile("/data/ethereum/contracts")
valid_contracts_ls = contracts_ls.filter(fl_contracts_fn)
map_contracts_ls = valid_contracts_ls.map(lambda x: (x.split(',')[0], None))

#get the intersection of transactions and contracts
joined_tbl = aggregate_transaction_ls.join(map_contracts_ls)
top_10_tbl = joined_tbl.takeOrdered(10, key = lambda x: -x[1][0])

duration = time.time() - start_time


with open('BDP_PART_D_cmpr_5.txt', 'w') as file:
	for each in top_10_tbl:
		file.write("{}:{}\n".format(each[0],each[1][0]))
	file.write("\nDuration = {}\n".format(duration))


