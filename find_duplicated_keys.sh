#!/usr/bin/env bash

# This script is used to clean the data set sent by the client from ambiguous values
# How to use:
# Execute the script with a list of csv files as options.
# The csv files must contain the list of the files to be cleaned in this way:
# filename,key_1_column_name (exactly like the header) ,key_2_column_name,value
# The csv file must have the header (File_,Columns)
#
# the csv file can contain from 1 to 4 key columns and one value column.
# The ouput of the script will be the set of data used as the input, cleaned from any repeated keys having different corresponding values,
# a log file having the list of the removed lines, and a backup of the impacted files. (Backup folder to be set in the options)
#
# The script needs to be used this way:
# ./clean_data.sh --files-list $files_list --data-path $client_data_folder --tmp-dir $tmp_dir --bcp-dir $bcp_files_location --log_file $log --clean-data
#
# All the paths need to be absolute paths.
# The option --clean-data can be removed, having it added will make the script clean the data,
# otherwise, it would just log the ambiguities without modifying the data
#
# Author : Walid Kardous
#########################################################################################################################
set -e

BCP=/tmp/backup
tmp_dir=/tmp
log_file=$tmp_dir/clean.log

usage() {
  echo Help: $0 [--files-list]\* [--data-path]\* [--tmp-dir] [--bcp-dir]
  echo "(*)" : mandatory
}

if [ "$#" -eq 0 ]
  then
  usage
  exit 1
fi

while [ -n "$1" ]; do
  case "$1" in
    --files-list)
    shift
    files_list=$1
    shift
    ;;
    --data-path)
    shift
    DATA_PATH=$1
    shift
    ;;
    --tmp-dir)
    shift
    tmp_dir=$1
    shift
    ;;
    --bcp-dir)
    shift
    BCP=$1
    shift
    ;;
    --log_file)
    shift
    log_file=$1
    shift
    ;;
    --clean-data)
    clean_data=true
    shift
    ;;
    *)
    usage
    exit 1
  esac
done

if [ ! -d $BCP ]
  then
  mkdir $BCP
fi

cd $DATA_PATH

rm -rf $log_file


for line in $(tail -n+2 $files_list)
do

  if [ ! -e *`echo $line | awk -F, '{print $1}'`* ]
    then
    continue
  fi

  nb_columns=`echo $line | awk -F, '{print NF-1}'`

  #Read files names and columns to be checked, if the file has 2 or 3 columns only, columns 3,4 or 4 will be ignored
  file=$(ls *`echo $line | awk -F, '{print $1}'`*)

  if [ ! -s $file ]
    then
    continue
  fi

  clmn_name1=`echo $line | awk -F, '{print $2}'`
  clmn_name2=`echo $line | awk -F, '{print $3}'`
  clmn_name3=`echo $line | awk -F, '{print $4}'`
  clmn_name4=`echo $line | awk -F, '{print $5}'`
  clmn_name5=`echo $line | awk -F, '{print $6}'`

  #Fetch columns orders based on names
  c1=`head -1 $file | dos2unix | awk -v column=$clmn_name1 -F'|' '{for(i=1;i<=NF;i++) {if($i == column) print(i)}}'`
  c2=`head -1 $file | dos2unix | awk -v column=$clmn_name2 -F'|' '{for(i=1;i<=NF;i++) {if($i == column) print(i)}}'`
  c3=`head -1 $file | dos2unix | awk -v column=$clmn_name3 -F'|' '{for(i=1;i<=NF;i++) {if($i == column) print(i)}}'`
  c4=`head -1 $file | dos2unix | awk -v column=$clmn_name4 -F'|' '{for(i=1;i<=NF;i++) {if($i == column) print(i)}}'`
  c5=`head -1 $file | dos2unix | awk -v column=$clmn_name5 -F'|' '{for(i=1;i<=NF;i++) {if($i == column) print(i)}}'`

  value_column=$(echo $line | cut -d, -f$(($nb_columns + 1)))
  echo "Checking ${file}(${value_column})..."

  #Extracts ambiguous values
  if (( $nb_columns == 2 ))
    then
    paste -d"|" <(cut -d\| -f$c1 $file) <(cut -d\| -f$c2 $file) | sort -u | cut -d\| -f1 | uniq -d > $tmp_dir/ambig_value
  else
    if (( $nb_columns == 3 ))
      then
      paste -d"|" <(cut -d\| -f$c1 $file) <(cut -d\| -f$c2 $file) <(cut -d\| -f$c3 $file) | sort -u | cut -d\| -f1,2 | uniq -d > $tmp_dir/ambig_value
    else
      if (( $nb_columns == 4 ))
        then
        paste -d"|" <(cut -d\| -f$c1 $file) <(cut -d\| -f$c2 $file) <(cut -d\| -f$c3 $file) <(cut -d\| -f$c4 $file) | sort -u | cut -d\| -f1,2,3 | uniq -d > $tmp_dir/ambig_value
      else
        if (( $nb_columns == 5 ))
          then
          paste -d"|" <(cut -d\| -f$c1 $file) <(cut -d\| -f$c2 $file) <(cut -d\| -f$c3 $file) <(cut -d\| -f$c4 $file) <(cut -d\| -f$c5 $file) | sort -u | cut -d\| -f1,2,3,4 | uniq -d > $tmp_dir/ambig_value
        fi
      fi
    fi
  fi

  #If there are no ambiguities, jump to the next loop
  if [ ! -s $tmp_dir/ambig_value ]
  then
    continue
  else
    echo "File ${file}(${value_column}) has ambiguities"
  fi


  #Lines having ambiguity
  if (( $nb_columns == 2 ))
    then
    for ambig in `cat $tmp_dir/ambig_value`; do awk -v column1=$c1 -v value=$ambig -F'|' '$column1==value {print NR"|"$0;}' $file >> $tmp_dir/lines ; done
  else
    if (( $nb_columns == 3 ))
      then
      for ambig in `cat $tmp_dir/ambig_value`
      do
        v1=`echo $ambig | awk -F'|' '{print $1}'`
        v2=`echo $ambig | awk -F'|' '{print $2}'`
        awk -v column1=$c1 -v column2=$c2 -v value1=$v1 -v value2=$v2 -F'|' '$column1==value1 && $column2==value2 {print NR"|"$0;}' $file >> $tmp_dir/lines
      done
    else
      if (( $nb_columns == 4 ))
        then
        for ambig in `cat $tmp_dir/ambig_value`
        do
          v1=`echo $ambig | awk -F'|' '{print $1}'`
          v2=`echo $ambig | awk -F'|' '{print $2}'`
          v3=`echo $ambig | awk -F'|' '{print $3}'`
          awk -v column1=$c1 -v column2=$c2 -v column3=$c3 -v value1=$v1 -v value2=$v2 -v value3=$v3 -F'|' '$column1==value1 && $column2==value2 && $column3==value3 {print NR"|"$0;}' $file >> $tmp_dir/lines
        done
      else
        if (( $nb_columns == 5 ))
          then
          for ambig in `cat $tmp_dir/ambig_value`
          do
            v1=`echo $ambig | awk -F'|' '{print $1}'`
            v2=`echo $ambig | awk -F'|' '{print $2}'`
            v3=`echo $ambig | awk -F'|' '{print $3}'`
            v4=`echo $ambig | awk -F'|' '{print $4}'`
            awk -v column1=$c1 -v column2=$c2 -v column3=$c3 -v column4=$c4 -v value1=$v1 -v value2=$v2 -v value3=$v3 -v value4=$v4 -F'|' '$column1==value1 && $column2==value2 && $column3==value3 && $column4==value4 {print NR"|"$0;}' $file >> $tmp_dir/lines
          done
        fi
      fi
    fi
  fi

  #Adding first line for the loggin having the ambiguous file header
  echo "logs/`echo ${file}|cut -d\. -f1`.ambig;$(date);Row|`head -1 $file`|CAUSE|CAUSE_CODE" >> $log_file

  #logging the ambiguous values
  if (( $nb_columns == 2 ))
    then
    awk -v c1=$(($c1+1)) -v c2=$(($c2+1)) -v file=`echo ${file}|cut -d\. -f1` -v column_name1=$clmn_name1 -v column_name2=$clmn_name2 -F'|' '{print "logs/"file".ambig;"d";"$0"| Ambiguous "column_name2" value for "column_name1": "$c1" = "$c2"| AMBIGUOUS_VALUE"}' "d=$(date)" $tmp_dir/lines >> $log_file
  else
    if (( $nb_columns == 3 ))
      then
      awk -v c1=$(($c1+1)) -v c2=$(($c2+1)) -v c3=$(($c3+1)) -v file=`echo ${file}|cut -d\. -f1` -v column_name1=$clmn_name1 -v column_name2=$clmn_name2 -v column_name3=$clmn_name3 -F'|' '{print "logs/"file".ambig;"d";"$0"| Ambiguous "column_name3" value for "column_name1" and "column_name2": "$c1"|"$c2" = "$c3"| AMBIGUOUS_VALUE"}' "d=$(date)" $tmp_dir/lines >> $log_file
    else
      if (( $nb_columns == 4 ))
        then
        awk -v c1=$(($c1+1)) -v c2=$(($c2+1)) -v c3=$(($c3+1)) -v c4=$(($c4+1)) -v file=`echo ${file}|cut -d\. -f1` -v column_name1=$clmn_name1 -v column_name2=$clmn_name2 -v column_name3=$clmn_name3 -v column_name4=$clmn_name4 -F'|' '{print "logs/"file".ambig;"d";"$0"| Ambiguous "column_name4" value for "column_name1" and "column_name2" and "column_name3" : "$c1"|"$c2"|"$c3" = "$c4"| AMBIGUOUS_VALUE"}' "d=$(date)" $tmp_dir/lines >> $log_file
      else
        if (( $nb_columns == 5 ))
          then
          awk -v c1=$(($c1+1)) -v c2=$(($c2+1)) -v c3=$(($c3+1)) -v c4=$(($c4+1)) -v c5=$(($c5+1)) -v file=`echo ${file}|cut -d\. -f1` -v column_name1=$clmn_name1 -v column_name2=$clmn_name2 -v column_name3=$clmn_name3 -v column_name4=$clmn_name4 -v column_name5=$clmn_name5 -F'|' '{print "logs/"file".ambig;"d";"$0"| Ambiguous "column_name4" value for "column_name1" and "column_name2" and "column_name3" and "column_name4" : "$c1"|"$c2"|"$c3"| "$c4" = "$c5"| AMBIGUOUS_VALUE"}' "d=$(date)" $tmp_dir/lines >> $log_file
        fi
      fi
    fi
  fi




  #backing up files before cleaned
  if [ ! -e $BCP/notLoaded_${file} ]
    then
    cp $file $BCP/notLoaded_${file}
  fi


  if [[ $clean_data == true ]]
    then
    #removing lines havivng ambiguous values filtered on corresponding column(s)
    if (( $nb_columns == 2 ))
      then
      for ambig in `cat $tmp_dir/ambig_value`; do awk -v column1=$c1 -v value=$ambig -F'|' '$column1!=value' $file > $tmp_dir/tmp_file ; cat $tmp_dir/tmp_file > $file; done
    else
      if (( $nb_columns == 3 ))
        then
        for ambig in `cat $tmp_dir/ambig_value`;
        do
          v1=`echo $ambig | awk -F'|' '{print $1}'`
          v2=`echo $ambig | awk -F'|' '{print $2}'`
          awk -v column1=$c1 -v column2=$c2 -v value1=$v1 -v value2=$v2 -F'|' '$column1!=value1 || $column2!=value2' $file > $tmp_dir/tmp_file ; cat $tmp_dir/tmp_file > $file
        done
      else
        if (( $nb_columns == 4 ))
          then
          for ambig in `cat $tmp_dir/ambig_value`;
          do
            v1=`echo $ambig | awk -F'|' '{print $1}'`
            v2=`echo $ambig | awk -F'|' '{print $2}'`
            v3=`echo $ambig | awk -F'|' '{print $3}'`
            awk -v column1=$c1 -v column2=$c2 -v column3=$c3 -v value1=$v1 -v value2=$v2 -v value3=$v3 -F'|' '$column1!=value1 || $column2!=value2 || $column3!=value3' $file > $tmp_dir/tmp_file ; cat $tmp_dir/tmp_file > $file
          done
        else
          if (( $nb_columns == 5 ))
            then
            for ambig in `cat $tmp_dir/ambig_value`;
            do
              v1=`echo $ambig | awk -F'|' '{print $1}'`
              v2=`echo $ambig | awk -F'|' '{print $2}'`
              v3=`echo $ambig | awk -F'|' '{print $3}'`
              v4=`echo $ambig | awk -F'|' '{print $4}'`
              awk -v column1=$c1 -v column2=$c2 -v column3=$c3 -v column4=$c4 -v value1=$v1 -v value2=$v2 -v value3=$v3 -v value4=$v4 -F'|' '$column1!=value1 || $column2!=value2 || $column3!=value3 || $column4!=value4' $file > $tmp_dir/tmp_file ; cat $tmp_dir/tmp_file > $file
            done
          fi
        fi
      fi
    fi
  fi
  #cleanup
  rm $tmp_dir/ambig_value
  rm $tmp_dir/lines

done


if [ -e $tmp_dir/tmp_file ]
  then
  rm $tmp_dir/tmp_file
fi

