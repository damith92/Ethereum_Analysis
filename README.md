# Ethereum_Analysis
Analysis of Ethereum related bigdata using hadoop and spark.

**Ethereum Analysis - Report**

**Damith Chamalke Senadeera**

# 1Part A

## 1.1Part 1

**Job Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1637317090236\_22242/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22242/)

The objective of this task was to produce a bar plot for the aggregate amount of transactions happened in each month from the transactions data set of Ethereum.

For this I obtained the transactions data set in Hadoop cluster and then wrote one Hadoop map reduce job to count each transaction pertaining to a month to get the sum at the end.

First, I extracted the timestamp of each transaction in the mapper and obtained the relevant year and the month. Next considering (Year, Month) as the key, I yielded 1 as the output for each record to get the count of the transactions. Then in the reducer where I received all the counts for each unique (Year, Month) key, I got the sum of the counts per key to obtain the aggregate value pertaining to that unique particular (Year, Month) key.

I added a combiner as well for this task, as taking the sum is an associative operation and the combiner can implement the same function of summing the input records and yielding the intermediate sums making it easier for the reducer to perform the total summing with reduced number of steps.

Below is the bar plot for the aggregate transactions for each month in each year.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192405248-a06d9f0c-425d-4461-a4e9-b5b1bb8594a3.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>

According to this plot, I can see that the total transactions have remained the same till February 2017 and afterwards increased quite rapidly to have the peak during January 2018. After the Peak , the amount has dropped down may be depicting the crypto currency bubble in 2018 before starting to hover again for the upcoming months.

## 1.2Part 2

**Job Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1637317090236\_22259/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22259/)

The objective of this task is to get the average value of transactions for each month using the transactions data set.

Here I extracted the timestamp of each transaction and obtained the relevant year and the month from that along with the value of each transaction in the mapper. Here I used float data type for the transaction to make sure the full value of the transaction is being used and held for the averaging, given the transaction value can be a large number. Then I yielded (Year, Month) as the key and the extracted transaction value and a counter 1 for counting the number of transactions.

Then in the mapper I received all the values and counts pertaining to each unique (Year, Month) key and then took the average by summing all the values and dividing it by the sum of counts.

Here also I introduced a combiner, although averaging is not directly associative, I could decompose the averaging process into an associative intermediate step as I'm giving the counts separately. So, in the combiner I get the sum of the values and the sum of the counts for each unique key and yield those 2 as the value where key being the unique (Year, Month). By this I have reduced the computational load on the reducer.

Below plot shows the average transactions for each month in each year.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192405468-a41d63ef-c0e1-418e-bb4c-497b4dbd407e.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>


The huge spike in average transaction value the August 2015 should be due to the lower amount of transactions as it was just after the introduction of Ethereum on 2015 July 30th. Then after that the transaction volume should have gone up exponential within a short period of time for the average transaction values to plummet. After the crypto bubble in 2018 since Ethereum got very popular, the transaction amount should have increased exponentially for the average transaction values to go down gradually.

# 2Part B

Objective of this task was to find the top 10 smart contracts by total Ethereum received.

For this task I have written 2 jobs in map reduce.

**Job 1 Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1648683650522\_6008/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_6008/)

This job is to perform repartition join and get the sum of the total Ether received for each to\_address related to contracts. As the input I use the transaction data set and the contracts data set. In the mapper function first, I check if the input is from Transaction data (which has 7 fields) or from Contracts data (which has 5 fields).

If it's from transaction data I extract the to\_address field and the value field and yield to\_address as the key and value and a flag 1 to identify that this input is from transactions data in the reducer. If it's from contracts data I extract the address field and yield the address as the key and a string "In" and a flag 2 as values to indicate that this is from contract data.

In the reducer for unique address keys I'm appending the values sent by the mapper if those values are sent from transactions table (by comparing the 2nd value of the values tuple yielded by the mapper to be 1) or else if the data is from contracts table I'm changing the flag in the reducer to true, to indicate that the address is in the contracts table (by comparing the 2nd value of the values tuple yielded by the mapper to be 2).

Then if the flag indicating that this address is from contracts table is set to True, then the sum of the collected transaction values is yielded for each unique contract address which is fed as the input to the 2nd job.

**Job 2 Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1637317090236\_22365/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22365/)

In this job, as the input I have the sum of the values of the unique contract addresses. So, in the mapper I just yield the address and the value as the value without a key because we need all these values and addresses in a single list to sort them to find the top 10.

In the reducer here I have sorted all the (Address, Total Value) pairs in descending order and extracted the top 10.

Here I have deployed a combiner also to sort and extract the top 10 as it will reduce the amount of data to be sorted in the reducer.

Then I run the 2 jobs using MRStep consecutively.

Here is the table of top 10 contracts based on total Ethereum received.

| **Rank** | **Contract Address** | **Total Ether Received (In Wei)** |
| --- | --- | --- |
| 1 | 0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444 | 8.42E+25 |
| 2 | 0xfa52274dd61e1643d2205169732f29114bc240b3 | 4.58E+25 |
| 3 | 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 4.56E+25 |
| 4 | 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 4.32E+25 |
| 5 | 0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8 | 2.71E+25 |
| 6 | 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd | 2.11E+25 |
| 7 | 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 1.56E+25 |
| 8 | 0xbb9bc244d798123fde783fcc1c72d3bb8c189413 | 1.20E+25 |
| 9 | 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 1.17E+25 |
| 10 | 0x341e790174e3a4d35b65fdc067b6b5634a61caea | 8.38E+24 |

# 3Part C

The objective of this task was to find the top 10 miners according to the size of the blocks mined.

I have accomplished this task using 2 map reduce jobs.

**Job 1 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1637317090236\_22418/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22418/)

In this job I have taken the Blocks data as the input and then in the mapper I have extracted the miner id and the block size for each record and yielded them making miner id as the key and block size as the value.

In the reducer I'm summing up all the block sizes to give the total block size each unique miner has mined. Then I yield that total block size to each unique miner address as the value and miner address as the key which will be the input to the next job.

**Job 2 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1637317090236\_22419/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22419/)

In this job, as the input I have the sum of the block sizes mined by each unique miner. So, in the mapper I just yield the miner's address and the value as the value without a key because I need all these values and miner addresses in a single list to sort them to find the top 10.

In the reducer here I have sorted all the (Miner Address, Total Block Size) pairs in descending order and extracted the top 10.

Here also I have deployed a combiner to sort and extract the top 10 as it will reduce the amount of data to be sorted in the reducer.

Then I run the 2 jobs using MRStep consecutively.

Below is the table for the top 10 miners and their total mined block sizes

| **Rank** | **Miner Address** | **Aggregate Block Size** |
| --- | --- | --- |
| 1 | 0xea674fdde714fd979de3edf0f56aa9716b898ec8 | 23989401188 |
| 2 | 0x829bd824b016326a401d083b33d092293333a830 | 15010222714 |
| 3 | 0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c | 13978859941 |
| 4 | 0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5 | 10998145387 |
| 5 | 0xb2930b35844a230f00e51431acae96fe543a0347 | 7842595276 |
| 6 | 0x2a65aca4d5fc5b5c859090a6c34d164135398226 | 3628875680 |
| 7 | 0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01 | 1221833144 |
| 8 | 0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb | 1152472379 |
| 9 | 0x1e9939daaad6924ad004c2560e90804164900341 | 1080301927 |
| 10 | 0x61c808d82a3ac53231750dadc13c777b59310bd9 | 692942577 |

# 4Part D - Scam Analysis

## 4.1Popular Scams

### 4.1.1Part 1

The objective of this part is to find the most lucrative scam happened by the transaction volume.

**Job Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1648683650522\_3100/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_3100/)

Here I take 2 inputs, namely the Scams.json file and the transactions data. The Scams.json file is inputted as an additional file using –file command for the mapper initialization. So, in the mapper\_init function I read the Scams.json file and prepare a global dictionary where the addresses are the keys and scam type is the value. Then in the mapper function I extract the To Address and the value for each record and check if that To Address is in the scams related global dictionary of addresses which I prepared during initialization. If that address is found in the scam related address dictionary, then I extract the scam type related to that address using the dictionary and yield the scam type as the key and the transaction value extracted as the value.

In the reducer then I sum up the transaction values for each unique scam type to find the Totals for each scam type.

Again I have introduced a combiner here as this a summing operation to reduce the load on the reducer.

Below is the table I obtain as the result.

| **Type** | **Total Transaction Value (In Wei)** |
| --- | --- |
| Scamming | 3.84E+22 |
| Phishing | 2.69E+22 |
| Fake ICO | 1.36E+21 |

Here I can see by transaction value the most lucrative scam type to be Scamming while Phishing comes 2nd and Fake ICO comes 3rd.

### 4.1.2Part 2

The objective of this part is to correlate the above findings with certainly known scams going offline/inactive.

**Job Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1648683650522\_3102/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_3102/)

Here also I take 2 inputs, namely the Scams.json file and the transactions data. The Scams.json file is inputted as an additional file using –file command for the mapper initialization. So in the mapper\_init function I read the Scams.json file and prepare two global dictionaries where the addresses are the keys and scam type is the value in the category dictionary while in the other dictionary which is the status dictionary, the key is the address and the value is the status of the scam. Then in the mapper function I extract the To Address and the value for each record and check if that To Address is in the scams related global dictionary of addresses which I prepared during initialization. If that address is found in the scam related address dictionary and also in the status dictionary, then I extract the scam type and status related to that address using the two dictionaries and yield the (scam type, status) as the key and the transaction value extracted as the value.

In the reducer then I sum up the transaction values for each unique (scam type, status) key to find the Totals for each scam type and status.

I also have introduced a combiner here as this a summing operation to reduce the load on the reducer. The output table I obtained is as follows

| **Type** | **Status** | **Total Transaction Amount (In Wei)** |
| --- | --- | --- |
| "Scamming" | Active | 2.21E+22 |
| "Scamming" | Offline | 1.63E+22 |
| "Scamming" | Suspended | 3.71E+18 |
| "Phishing" | Active | 4.53E+21 |
| "Phishing" | Inactive | 1.49E+19 |
| "Phishing" | Offline | 2.24E+22 |
| "Phishing" | Suspended | 1.64E+18 |
| "Fake ICO" | Offline | 1.36E+21 |

Here I can see that for Scamming type Active status has the highest value and for Phishing type Offline status has the highest value where for Fake ICO, I can only find Offline status.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192405718-0695f902-6697-4a88-ae50-3cf46862f014.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>

## 4.2Wash Trading

Was Trading has been defined as "Entering into, or purporting to enter into, transactions to give the appearance that purchases and sales have been made, without incurring market risk or changing the trader's market position" by the Commodity Futures Trading Commission. [1].

Basically, if the balance of the trader remains unchanged for a given period of time, given that the trader has done many transactions can be considered as wash trading.

To find the wash traders in the data set I chose a fixed time frame of a week and a day and I try to find during that week or the day , who's net balance did not change.

### 4.2.1Part 1

**Job 1 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_3668/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_3668/)

**Job 2 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_3777/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_3777/)

**Job 3 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_3781/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_3781/)

Here I try to fix the time window as 1 unique week. Here I get the transaction data as input and if the value of the record is more than zero, I will extract the From Address and To Address and yield 2 times , 1st one (From Address, Year, Week of the Year) being the key and value and a flag 1 as values to identify it's for a From Address. For the 2nd yield, I take the (To Address, Year, Week of the Year) as the key and Value and a flag 2 to identify it's from a To Address.

In the reducer, for a unique address for a unique Week in a given year, I get the total received value and the sent value and try to see the net balance of the value by comparing the received value and the sent value and if they are equal, I yield that address and the summed-up value as a wash trade with the address as the key and the transaction value as the value.

In the job 2, I basically sum up all such wash trade values gathered for a unique address and in the job 3 I sort them in the descending order of the cumulative value.

Top 5 addresses for net zero balance in a time frame of a week.

| **Address** | **Cumulative Value (In Wei)** |
| --- | --- |
| 0xba0fc403ae9379763e6f864cdc470139ebdd774e | 5.52E+23 |
| 0x2cc6d6f3701dfced7efefae71b51d281cc67f497 | 3.77E+23 |
| 0x6ea853c1f4897b2a85ea1c16acd0eca6f88bc205 | 3.70E+23 |
| 0x8efc337a1e5748f28d819dff77f14f65356345b2 | 3.67E+23 |
| 0xd14c70863b21a8902476d671db9e5fae53e96a12 | 3.64E+23 |

###

### 4.2.2Part 2

**Job 1 Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_3979/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_3979/)

**Job 2 Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_4091/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_4091/)

**Job 3 Link -** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_4095/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_4095/)

I perform the same operations as above just changing the time window to 1 day to see who are the wash traders with largest values for a given unique day.

Top 5 addresses for net zero balance in a time frame of a day.

| **Address** | **Cumulative Value (In Wei)** |
| --- | --- |
| 0xba0fc403ae9379763e6f864cdc470139ebdd774e | 6.76E+23 |
| 0x60e16961ad6138d2fb3e556fc284d9c2fff41486 | 6.31E+23 |
| 0xc1de11585557c6b26272759cb148170d832ab7a2 | 4.40E+23 |
| 0xd14c70863b21a8902476d671db9e5fae53e96a12 | 3.71E+23 |
| 0x2cc6d6f3701dfced7efefae71b51d281cc67f497 | 3.69E+23 |

Here we can see that the address "0xba0fc403ae9379763e6f864cdc470139ebdd774e" for the highest aggregated value for weekly wash trade filtering and daily wash trade filtering are the same.

There are some limitations in the analysis I performed. I have taken a fixed window of a week and a day, but to identify wash traders accurately I think I need to implement a sliding window and check for the consistency of the balance. Also, a wash trader might not get all the value back what is sent, so maybe I could have introduced a threshold of

_(total\_sent\_value – total\_received\_value) \<= total\_sent\_value \* 0.001_

Therefore, this data is accurate only for the ideal wash trade scenario and in reality, there might be much bigger wash traders under a small flexible threshold of net balance.

# 5Part D - Miscellaneous Analysis

## 5.1Fork The Chain

The objective of this is to identify a fork and it's effects on price and general usage and find out who profited the most.

I have decided to analyze the effect of the Byzantium fork happened on 16th October 2017. It has been discussed as an important fork in the Ethereum to improve the Security, Scalability and Privacy of Ethereum transactions. According to the Ethereum website, 4 major points have been achieved through this fork. [2]

- The block mining reward has been reduced to 3 from 5 ETH.
- The difficulty bomb has been delayed by 1 year.
- Non state changing calls to other contracts have been enabled.
- Cryptography methods have been added to allow layer two scaling.

To analyze the effect of this Byzantium fork,

### 5.1.1Part 1

As the 1st part I thought of finding the average gas price behavior and the amount of total transactions happened before and after the Byzantium fork.

**Job Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_2959/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_2959/)

For this I got the transactions data as the input and extracted the gas price and the month, date and the year value for that transaction record. Then if that transaction happened in the year 2017 and in October, I yielded the data as the key and the gas price and a counter 1 as the values to get the total amount of transactions and the average gas price later.

In the reducer I obtained the sum of the gas prices and the transactions and then got the average gas price for each unique date in the month of October in 2017.

I also used a combiner here to get the intermediate total for gas prices and counts for unique days, as it will reduce the burden of the reducer.

The following 2 plots are obtained from the data I yielded from the reducer.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192405978-61d84165-8f75-45a3-9521-52324629703b.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>


I can see that the total number of transactions have surged and increased gradually after 16th October 2017

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406000-1b329791-cafb-4622-b1ef-041cc08d5089.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>


And also, the average gas price has plummeted and decreased gradually after 16th October 2017 as the mining reward has been reduced to 3 ETH from 5.

### 5.1.2Part 2

Here my objective is to find out who profited the most out of the Byzantium fork. Since it happened almost in the middle of the month of October 2017, I got the transaction values for the October 2017 and tried to find the net balance of the transactions for each address and sort the top 10 addresses with highest profit. For this I designed 2 jobs in map reduce.

**Job 1 link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_2965/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_2965/)

Here in the mapper, I get the transactions data as input and extract the value 1st. If the value is greater than 0 then I extract the day, month and the year and check if the year is 2017 and the month is October. If so, I extract the To Address and the From Address and yield 2 separate values where one with key as the To Address and value as the positive value of the extracted value field and the other yield where key is the From Address and the value as the negative value of the extracted value field as it counts as a reduction of value from the From Address.

Then in the reducer I get the sum of the values I get from the mapper for each unique address, since I pass negative value for the values going out of the address and positive values for the values I receive for the address, I get the net balance of the address for this particular month October 2017.

**Job 2 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_2967/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_2967/)

Here in the mapper, I get the output of the 1st reducer as the input and pass them as a list of values to sort them out to find the top 10.

In the 2nd reducer I then sort the values according to the descending order of the highest total value obtained by each address.

Here also I have implemented a combiner to intermediately find the top 10 and pass it to the reducer.

Then I run the 2 jobs using MRStep consecutively.

The following 10 addresses seem to have profited the most in October 2017, possibly due to the Byzantium fork.

| **Address** | **Profit (In Wei)** |
| --- | --- |
| 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 2.72E+24 |
| 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 8.84E+23 |
| 0xfa52274dd61e1643d2205169732f29114bc240b3 | 6.71E+23 |
| 0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8 | 6.24E+23 |
| 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 5.51E+23 |
| 0x8d12a197cb00d4747a1fe03395095ce2a5cc6819 | 4.24E+23 |
| 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 2.67E+23 |
| 0x37c1061324a719fba38ca466930be8323b5474c7 | 2.43E+23 |
| 0xf03b41d34635db6506948980b843390f8bd8e144 | 2.43E+23 |
| 0xf4b51b14b9ee30dc37ec970b50a486f37686e2a8 | 2.08E+23 |

## 5.2Gas Guzzlers

### 5.2.1Part 1

The objective of this part is to find how has gas price changed over time.

**Job Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1637317090236\_23037/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_23037/)

Here I have taken the transactions data as the input and extracted the gas price and the month and year value from the time stamp and yielded (Year, Month) as the key and gas price and a counter 1 as values to get the average gas price for each month in the reducer.

In the reducer I have taken the average gas price for each unique (Year, Month) key and yielded that.

I also implemented a combiner here to get the intermediate totals of the gas price and the counts to reduce the burden on reducer.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406148-6be9aea5-acc2-4838-a6f3-f62b65ae81e7.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>

Here I can see at the very beginning the gas price is very high and suddenly within 1 month it has plummeted. This may be because as soon as Ethereum was introduced the price of Ethereum was considerably low, and within short time it surged drastically, so then you needed only a much small amount of Ethereum to transfer the same value compared to earlier.

### 5.2.2Part 2

The objective of this part is to find out if the contracts have become more complicated requiring more gas or not.

For this task I wrote 2 map reduce jobs.

**Job 1 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1648683650522\_7300/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_7300/)

Here I get the block data and contract data as my inputs and if the data record is from block data I extract the block number, difficulty, gas used along with month and the year of the timestamp of the record and yield block number as the key and Year, Month, gas used, difficulty and a flag 0 to identify that this record is from block data. If the record is from contracts, I extract the block number and yield it as the key and a flag 1 as value to identify that this record is from contract data.

In the reducer, I perform the repartition join and yield the (Year, Month) as the key and gas used and the difficulty as the value only if the block is mentioned in the contracts data.

**Job 2 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1648683650522\_7307/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_7307/)

Here I get the input of the previous job and calculate the average for each unique (Year, Month) Key with the help of a combiner as well.

Below is the plot obtained from the results.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406341-129710c4-9d19-4559-a8f7-35505b4316dd.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>


<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406398-44d484db-a315-47fc-b865-51c784badc01.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>


I can see in general the average gas used has increased over time. The average difficulty has also increased generally till 2018 August, but afterwards it has fluctuated.

### 5.2.3Part 3

Here the objective is to corelate the complexity of the top 3 contract found in part B with the change of their transactions. To depict the change of their transactions, I'm getting the average gas price related to these top 3 contract since if more transactions have happened for a contract, more gas price should have incurred.

**Job 1 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_0799/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_0799/)

Here I take transactions and block data as my main inputs and in addition I give the top 10 contract addresses I found in part B as a separate input to the map init of this job. Here in the map init, I prepare a list of top 10 transactions I found earlier in part B and in the mapper when a transaction record is inputted I check if the To Address of that record is in the top 3 of those contract addresses. If so only I will proceed to extract the block number, gas price and the month and the year and yield Block Number as the key and gas price and (Year, Month) as values. If I encounter a block record, I just yield the Block number as the key and difficulty and the (year, Month) as values.

In the reducer I perform a repartition join to filter out the specific blocks only related to these top 3 contracts and yield (Year, Month) as the key and gas price and a flag 0 and difficulty and flag 1 to identify those 2 outputs separately in the next step.

**Job 2 Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application\_1649894236110\_0806/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_0806/)

Here I get the output of the previous job as the input and average the difficulty and gas price for each unique (Year, Month) with the help of a combiner as well.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406577-15c16877-d3c3-401e-980a-07607bd919a4.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>

The average gas price for the top 3 contract shows somewhat of a similar pattern for what I obtained in the 1st step of this question.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406626-904c8b82-1132-4ac7-b771-fff3b01db650.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>

The average difficulty change over time clearly shows a similar pattern to the Average difficulty results I obtained in the part 2 of this question.

## 5.3Comparative Evaluation

Here the objective is to run the spark job for the same task of part B and compare the execution times.

**Spark Job Link –** [http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/redirect/application\_1648683650522\_514/](http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/redirect/application_1648683650522_514/)

Here the spark job also uses Transactions data and I extract the To Address and the Value validating the records and then from the contracts data The address validating the contract records. Then I join the 2 extracted data set with the Address as the key and sort and take the top 10 contracts.

I also print the total time taken to execute the spark job.

I executed this same job 5 times and here is the average execution time.

**Average spark execution time** = (107.08156085 + 175.979979038 + 131.410804987 + 220.1038239 + 153.768636942) / 5 = **157.668961143 seconds** = **2.63 minutes**

For the Hadoop map reduce job I took the execution time using the statistics at the end of each job by taking the sum of the total time spent by mappers / number of total mappers and total time spent by reducers / number of total reducers.

I ran this also 5 times.

**Average Hadoop Map Reduce execution time** = ((941.3192681 + 62.7755) + (928.3762382 + 70.221) + (911.0383687 + 70.431) +( 875.407865 + 69.54375) + (975.0526042 + 63.7793775))/ 5 = **993.58899434 seconds** = **16.56 minutes**

If we compare the two execution times, Spark average execution time is only around 15.87% of the Hadoop Map Reduce average execution time.

<p>&nbsp;</p>
<kbd>
<img src="https://user-images.githubusercontent.com/14356479/192406703-b226afec-6fa0-4a70-81f8-dcce42641616.jpg"  width="1000" ></kbd>
<p>&nbsp;</p>


# 6References

[1] Victor, F., & Weintraud, A. M. (2021, April). Detecting and quantifying wash trading on decentralized cryptocurrency exchanges. In Proceedings of the Web Conference 2021 (pp. 23-32).

[2] [https://ethereum.org/en/history/#byzantium](https://ethereum.org/en/history/#byzantium)
