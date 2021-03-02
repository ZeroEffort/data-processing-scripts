import csv
import gzip



filePath = "attributes_sku_replen.gz"
with gzip.open(filePath, "r") as csvfile:
	with open("combine_sales_skus", "w") as file:
		reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
		next(reader)
		store_replen_skus = []
		for row in reader:
			sku = row[0]
			if row[15] == "N":
				store_replen_skus.append(sku)
			if row[16] == "N":
				file.write(str(sku)+"\n")

store_replen_skus = frozenset(store_replen_skus)
filePath = "attributes_skuloc_replen.gz"

print ""
boh_excluded = ["ST301199","ST100946", "ST301946"]
count = 0
with gzip.open(filePath, "r") as csvfile:
	with open("boh_intersections", "w") as file:
		writer = csv.writer(file, delimiter="|")
		reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
		next(reader)
		for row in reader:
			if row[1] in boh_excluded:
				continue
			if row[6] == "N":
				if row[0] in store_replen_skus:
					new_row = [row[0], row[1]]
					writer.writerow(new_row)
			count += 1 
			if count % 100000 == 0:
				print count

			
with open("warehouse_shift.dat", "r") as csvfile:
	with open("store_shift", "w") as file:
		writer = csv.writer(file, delimiter="|")
		reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
		next(reader)
		for row in reader:
			if row[3] == "Y":
				new_row = [row[0], row[2], row[1]]
				writer.writerow(new_row)
