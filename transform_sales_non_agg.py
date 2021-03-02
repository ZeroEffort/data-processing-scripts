import os
import csv
import gzip
import time


# reading boh intersections
boh_mapping_file = "store_servicing_dc.csv"
boh_mapping = {}
boh_sku_list = []
with open(boh_mapping_file, "r") as csvfile:
	reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
	for row in reader:
		sku = row[0]
		from_store = row[1]
		to_store = row[2]
		if sku not in boh_mapping:
			boh_mapping[sku] = {from_store:to_store}
			boh_sku_list.append(sku)
		else:
			boh_mapping[sku][from_store]=to_store
	boh_sku_set = frozenset(boh_sku_list)	

store_shift_file = "store_shift"
store_shift_mapping = {}
with open(store_shift_file, "r") as csvfile:
	reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
	for row in reader:
		from_loc = row[0]
		state = row[1]
		to_loc = row[2]
		if from_loc not in store_shift_mapping:
			store_shift_mapping[from_loc] = {state:to_loc}
		else:
			store_shift_mapping[from_loc][state]=to_loc


combine_sales_skus_file = "combine_sales_skus" 
combine_sales_skus = []
with open(combine_sales_skus_file, "r") as csvfile:
	for row in csvfile:
		combine_sales_skus.append(row.rstrip('\n'))
	combine_sales_skus = frozenset(combine_sales_skus)

day2week_file = "day2week.csv"
day2week = {}
with open(day2week_file, "r") as csvfile:
	reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
	for row in reader:
		day2week[int(row[0])] = int(row[1])


header = ["Week","SKU","Store","Day","State","unitsReg","unitsClear","salesReg","salesClear"]
with open("sales_daily.dat", "r") as csvfile:
	reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
	next(reader)
	count = 0
	start = time.time()
	with open("non_agg_shifted_sales.csv", "w") as store_file:
		writer = csv.writer(store_file, delimiter="|")
		writer.writerow(header)
		for row in reader:
			if row[4] == "ST100902":
				continue
			# boh aggregation
			if row[1] in boh_sku_set:
				if row[3] in boh_mapping[row[1]]:
					row[3] = boh_mapping[row[1]][row[3]]
			# whs shift
			if row[3] in store_shift_mapping:
				if row[1] in combine_sales_skus:
					if row[5] in store_shift_mapping[row[3]]:
						row[3] = store_shift_mapping[row[3]][row[5]]
			week = day2week[int(row[0])]
			if row[6] == "R":
				new_row = [week, row[1], row[3], row[5], row[9], 0, row[7], 0]
			else:
				new_row = [week, row[1], row[3], row[5], 0, row[9], 0, row[7]]

			writer.writerow(new_row)
			#print time.time() - start
			count += 1
			if count % 1000000==0:
				print count
				print time.time() - start

