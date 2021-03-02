import sys
import csv
import time
import argparse

#Week|SKU|Store|unitsReg|unitsClear|salesReg|salesClear


parser = argparse.ArgumentParser(description='Compare restated and prod Sales.')
parser.add_argument('-o', '--old', dest='file1', action='store', default="dev4_sales.csv", help='first sales file to compare')
parser.add_argument('-n', '--new', dest='file2', action='store', default="dev3_sales.csv", help='second sales file to compare')
parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='print more status messages to stdout', default = False)
args = parser.parse_args()

verbose = args.verbose
file1 = args.file1
file2 = args.file2


def compare_intersections(old_set, new_set, Log_flag=True):
	intersections_set = old_set.intersection(new_set)
	removed_intersections = old_set - intersections_set
	added_intersections = new_set - intersections_set
	fileName = "intersections_count_summary"
	with open(fileName, "w") as reportFile:
		reportFile.write("-------------------------------------------------------------------------\n")
		reportFile.write("                    Intersctions Count Summary    \n")
		reportFile.write("-------------------------------------------------------------------------\n")
		reportFile.write("Original sales Intersections Count: {0} \n".format(len(old_set)))
		reportFile.write("New sales Intersections Count     : {0} \n".format(len(new_set)))
		reportFile.write("Removed sales Intersections Count : {0} \n".format(len(removed_intersections)))
		reportFile.write("Added sales Intersections Count   : {0} \n".format(len(added_intersections)))
	if verbose:
		print ("-------------------------------------------------------------------------\n")
		print ("                    Intersctions Count Summary    \n")
		print ("-------------------------------------------------------------------------\n")
		print ("Original sales Intersections Count: {0}".format(len(old_set)))
		print ("New sales Intersections Count     : {0}".format(len(new_set)))
		print ("Removed sales Intersections Count : {0}".format(len(removed_intersections)))
		print ("Added sales Intersections Count   : {0}".format(len(added_intersections)))
	if Log_flag:
		with open("removed_intersections.csv", "w") as reportFile:
			reportFile.write("Week|SKU|Store\n")
			for line in removed_intersections:
				reportFile.write(line + "\n")

		with open("added_intersections.csv", "w") as reportFile:
			reportFile.write("Week|SKU|Store\n")
			for line in added_intersections:
				reportFile.write(line + "\n")
	return added_intersections, removed_intersections, intersections_set










print "reading product sales file"
prod_records = {}
with open(file1, "r") as csvfile:
	reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
	next(reader)
	start = time.time()
	count = 0
	for row in reader:
		key = row[0]+"|"+row[1]+"|"+row[2]
		prod_records[key] = (float(row[3]), float(row[4]), float(row[5]), float(row[6]))
		count+=1
		# if count % 1000000==0:
		# 	print count
		# 	print time.time() - start
		# 	start = time.time()

	prod_intersections = frozenset(prod_records.keys())

print "reading Restated sales file"
restated_records = {}
with open(file2, "r") as csvfile:
	reader = csv.reader(csvfile, delimiter="|", skipinitialspace=True)
	next(reader)
	start = time.time()
	count = 0
	for row in reader:
		key = row[0]+"|"+row[1]+"|"+row[2]
		restated_records[key] = (float(row[3]), float(row[4]), float(row[5]), float(row[6]))
		count+=1
		# if count % 1000000==0:
		# 	print count
		# 	print time.time() - start
		# 	start = time.time()

	restated_intersections = frozenset(restated_records.keys())




print "comparing Intersections"
start = time.time()
added_intersections, removed_intersections, intersections_set = compare_intersections(prod_intersections, restated_intersections, Log_flag=True)
print time.time() - start

del removed_intersections
del added_intersections


print "Comparing common intersections"
th = 0.001
count = 0
with open("differences.csv", "w") as file:
	writer = csv.writer(file, delimiter="|", escapechar=' ', quoting=csv.QUOTE_NONE)
	writer.writerow(["Week|SKU|Store|Prod_unitsReg|Rest_unitsReg|Prod_unitsC|Rest_unitsC"])
	for key in intersections_set:
		prod_tuple = prod_records[key]
		restated_tuple = restated_records[key]
		if ( (abs(prod_tuple[0] - restated_tuple[0]) > th) or (abs(prod_tuple[1] - restated_tuple[1]) > th) or (abs(prod_tuple[2] - restated_tuple[2]) > th) or (abs(prod_tuple[3] - restated_tuple[3]) > th) ) :
			new_row = [key] + [prod_tuple[0], restated_tuple[0], prod_tuple[1], restated_tuple[1]]
			writer.writerow(new_row)
		count +=1
		# if count % 1000000==0:
		# 		print count
		# 		print time.time() - start
		# 		start = time.time()

 


print "-------------------------------------------"
print "Done !"
sys.exit()