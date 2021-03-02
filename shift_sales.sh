#!/usr/bin/env bash
set -ex

while getopts ":i:f:" opt
   do
     case $opt in
     	i ) s3folder=$OPTARG;;
		f ) filterskus=$OPTARG;;
     esac
done

if [ -z "$s3folder" ]
then
      echo "no s3 folder specified"
      echo "will use local files"
fi

if [ -z "$filterskus" ]
then
      echo "no filter skus file specified"
      echo "will run on all skus"
fi



if [ ! -z "$s3folder" ]
then
	# downloading attributes_sku_replen.gz
    cloud-store download $s3folder/stage_archive/20201101/backup/Attributes/attributes_sku_replen.gz -o . --overwrite
    # downloading attributes_skuloc_replen.gz
    cloud-store download $s3folder/stage_archive/20201101/backup/Attributes/attributes_skuloc_replen.gz -o . --overwrite
    # downloading warehouse shift s3://crate-mfm-dev/OneEcomm/SalesHistory/to_load/warehouse_shift_20210114_000001.zip 
    cloud-store download s3://crate-mfm-dev/OneEcomm/SalesHistory/to_load/warehouse_shift_20210114_000001.zip  -o . --overwrite
    unzip warehouse_shift_20210114_000001.zip
    # downloading the sales_forecast file
    cloud-store download $s3folder/engine_data/20201101/fe_weekly/input/common_input/sales_forecasting.dat  -o . --overwrite
fi

lb print /cb/stage pdx:cal:day_week --raw --exclude-ids | tr " " "|" > day2week.csv

export_boh_shift=true

if [ "$export_boh_shift" = true ] ; then
    python gen_boh_intersections.py

    cat boh_intersections | awk -F "|" '{print substr($0, 1, length($0)-1)}' > boh_filter
	cat boh_intersections | awk -F "|" '{OFS="|"; print "\""$1"\"" OFS "\""substr($2, 1, length($2)-1) "\""}'  | cut -d "|" -f2 | sort -u  > store_list

	echo 'echo "sku|store|dc" > store_servicing_dc' > queries.sh 
	while IFS="|" read -r field1
	do
		echo "echo 'store : $field1' " >> queries.sh 
		echo "lb query /cb/stage ' _[sku, store] = dc <-	staging:actuals_ty:sales_aggs:_servicing_dc[store, sku] = dc, pdx:loc:location_id[store] = $field1 . ' --exclude-ids --delimiter '|' --csv --raw "  >> queries.sh
		echo "cat _.csv >> store_servicing_dc" >> queries.sh 
	done < store_list

	echo 'grep -f boh_filter store_servicing_dc > store_servicing_dc.csv' >> queries.sh 
	chmod +x queries.sh
	./queries.sh
else
	python gen_boh_intersections.py
	cat boh_intersections | awk -F "|" '{print substr($0, 1, length($0)-1)}' > boh_filter
	## If export_boh_shift is set to false, then how store_servicing_dc will be created.
	grep -f boh_filter store_servicing_dc > store_servicing_dc.csv
fi
rm boh_filter

get_the_sales=true
if [ "$get_the_sales" = true ] ; then

	if [ -f sales_daily.dat ] ; then
		rm sales_daily.dat
	fi

	sales_files=$(cloud-store ls "s3://crate-mfm-dev/OneEcomm/SalesHistory/to_load/" | grep -o -E "sales_daily_[0-9]*_[0-9]*")
	for file in $sales_files
	do
		echo "downloading $file"
		cloud-store download s3://crate-mfm-dev/OneEcomm/SalesHistory/to_load/$file.zip -o . --overwrite
		if [ ! -f sales_daily.dat ] ; then
			zcat $file.zip > sales_daily.dat
		else
			zcat $file.zip | tail -n +2  >> sales_daily.dat
		fi
		rm $file.zip
	done	

	if [ ! -z "$filterskus" ]
	then
	      grep -f $filterskus sales_daily.dat > tmp_sales
	      mv tmp_sales sales_daily.dat
	fi

fi

echo "Transforming the sales"
time python transform_sales_non_agg.py

echo "Aggregating the sales"

time awk -F "|" '{ OFS="|"; k = $1 OFS $2 OFS $3  }{ sum1[k] += $5; sum2[k] += $6; sum3[k] += $7; sum4[k] += $8; count[k]++ } END{ OFS="|"; for (i in sum1) print i, sum1[i], sum2[i], sum3[i], sum4[i] }' non_agg_shifted_sales.csv > agg_sales.csv

exit 0