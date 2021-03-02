'''
To run the script we need the 2 forecast files to compare called here old and new and the location hierarchy file
example command run

forecast_comparaison.py -o forecast_long.csv -n forecast_long2.csv -l location_hierarchy.dat -v -t 20

'''


import pandas as pd
import time
import argparse




parser = argparse.ArgumentParser(description='Compare restated and prod forecast.')
parser.add_argument('-o', '--old', dest='old_forecast_file', action='store', default="forecast_long.csv", help='first forecast file to compare')
parser.add_argument('-n', '--new', dest='new_forecast_file', action='store', default="forecast_long2.csv", help='second forecast file to compare')
parser.add_argument('-l', '--loc', dest='location_hierarchy', action='store', default="location_hierarchy.dat", help='location_hierarchy file')
parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='print more status messages to stdout', default = False)
parser.add_argument('-t', '--threshold', dest='threshold', action='store', type=float, default=10, help='threshold fore forecast abusers')
args = parser.parse_args()

new_forecast_file = args.new_forecast_file
old_forecast_file = args.old_forecast_file
location_hierarchy = args.location_hierarchy
tableau_report_file = "tableau_report_channel.csv"
verbose = args.verbose
Threshold = args.threshold


def compare_intersections(old_set, new_set, Log_flag=False, loc_type="Store"):
	intersections_set = old_set.intersection(new_set)
	removed_intersections = old_set - intersections_set
	added_intersections = new_set - intersections_set
	fileName = loc_type + "_intersections_count_summary.csv"
	with open(fileName, "w") as reportFile:
		reportFile.write("-------------------------------------------------------------------------\n")
		reportFile.write("                    Intersctions Count Summary    \n")
		reportFile.write("-------------------------------------------------------------------------\n")
		reportFile.write("Original Forecast Intersections Count: {0} \n".format(len(old_set)))
		reportFile.write("New Forecast Intersections Count     : {0} \n".format(len(new_set)))
		reportFile.write("Removed Forecast Intersections Count : {0} \n".format(len(removed_intersections)))
		reportFile.write("Added Forecast Intersections Count   : {0} \n".format(len(added_intersections)))
	if verbose:
		print ("-------------------------------------------------------------------------\n")
		print ("                    Intersctions Count Summary    \n")
		print ("-------------------------------------------------------------------------\n")
		print ("Original Forecast Intersections Count: {0}".format(len(old_set)))
		print ("New Forecast Intersections Count     : {0}".format(len(new_set)))
		print ("Removed Forecast Intersections Count : {0}".format(len(removed_intersections)))
		print ("Added Forecast Intersections Count   : {0}".format(len(added_intersections)))
	if Log_flag:
		with open(loc_type + "_removed_intersections.csv", "w") as reportFile:
			reportFile.write("Sku|" + loc_type + "|week\n")
			for line in removed_intersections:
				reportFile.write(line + "\n")

		with open(loc_type + "_added_intersections.csv", "w") as reportFile:
			reportFile.write("Sku|" + loc_type + "|week\n")
			for line in added_intersections:
				reportFile.write(line + "\n")
	return added_intersections, removed_intersections, intersections_set



print "reading location_hierarchy file"
location_hierarchy = pd.read_csv(location_hierarchy, sep="|", usecols = ["Store", "MerchChannel"])
location_hierarchy = location_hierarchy.rename(columns = {"Store":"store"})

start_time = time.time()

print "loading original forecast file"
old_forecast = pd.read_csv(old_forecast_file, sep="|")
print("--- %s seconds ---" % (time.time() - start_time))
old_forecast = old_forecast.rename(columns = {"baseline":"orig_baseline", "forecast":"orig_forecast"})

print "merging old forecast file with the location hierarchy"
old_forecast = pd.merge(old_forecast, location_hierarchy, on = "store", how = "left")

print "aggregating old forecast"
start_time = time.time()
old_channel = old_forecast.groupby( ["sku", "MerchChannel", "week"], as_index=False).agg({"orig_baseline": "sum", "orig_forecast": "sum"})
print("--- %s seconds ---" % (time.time() - start_time))




print "loading new forecast file"
new_forecast = pd.read_csv(new_forecast_file, sep="|")
print("--- %s seconds ---" % (time.time() - start_time))
new_forecast = new_forecast.rename(columns = {"baseline":"new_baseline", "forecast":"new_forecast"})
start_time = time.time()

print "merging new forecast file with the location hierarchy"
new_forecast = pd.merge(new_forecast, location_hierarchy, on = "store", how = "left")

print "aggregating new forecast"
start_time = time.time()
new_channel = new_forecast.groupby( ["sku", "MerchChannel", "week"], as_index=False).agg({"new_baseline": "sum", "new_forecast": "sum"})
print("--- %s seconds ---" % (time.time() - start_time))

print "merging new and old forecast at channel level"
start_time = time.time()
data = pd.merge(new_channel, old_channel, on = ["sku", "MerchChannel", "week"], how = "inner")
print("--- %s seconds ---" % (time.time() - start_time))
data["diff_baseline"] = data["new_baseline"] - data["orig_baseline"]
data["diff_forecast"] = data["new_forecast"] - data["orig_forecast"]
data.to_csv(tableau_report_file, sep="|", index= False, float_format='%.5f')
print("--- %s seconds ---" % (time.time() - start_time))

print "Calculating Metrics"
abs_error_forecast = abs(data["diff_forecast"])
abs_error_baseline = abs(data["diff_baseline"]) 
wape_forecast = abs_error_forecast.sum() / data["orig_forecast"].sum()
wape_baseline = abs_error_baseline.sum() / data["orig_baseline"].sum()

if verbose:
	print ("\n\n-------------------------------------------------------------------------\n")
	print ("                    Difference Metrics at {0} level   \n".format("store "))
	print ("-------------------------------------------------------------------------\n")
	print ("Weighted Absolute Percent Difference of Baseline: : {0} \n".format(wape_baseline))
	print ("Weighted Absolute Percent Difference of Forecast: : {0} \n".format(wape_forecast))



print "\n\nComparing intersections"
new_intersections = frozenset(new_forecast["sku"] + "|" + new_forecast["store"] + "|" + new_forecast["week"].astype(str) )
old_intersections = frozenset(old_forecast["sku"] + "|" + old_forecast["store"] + "|" + old_forecast["week"].astype(str) )
added_intersections, removed_intersections, intersections_set = compare_intersections(old_intersections, new_intersections, Log_flag=True, loc_type="Store")


print "Getting worst forecast abusers"
forecast_abusers = data[data["diff_forecast"] >= Threshold][["sku","MerchChannel"]].drop_duplicates()
forecast_abusers.to_csv("forecast_abusers.csv", sep="|", index= False, float_format='%.5f')


# print "merging new and old forecast at store level"
# start_time = time.time()
# data = pd.merge(new_forecast, old_forecast, on = ["sku", "MerchChannel", "week"], how = "inner")
# print("--- %s seconds ---" % (time.time() - start_time))
# data["diff_baseline"] = data["new_baseline"] - data["orig_baseline"]
# data["diff_forecast"] = data["new_forecast"] - data["orig_forecast"]
# print("--- %s seconds ---" % (time.time() - start_time))