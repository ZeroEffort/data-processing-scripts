import argparse
import glob
import os
import subprocess
import datetime 

def cloud_store_upload(input, dest, dry_run, encryption_key):
    cmd = 'cloud-store upload --key %s -i %s %s' % (encryption_key, input, dest)
    print('DRY RUN:' + cmd)
    if dry_run is not True:
        subprocess.check_call(cmd, shell=True)
    return

def rm_timestamp_gz(pattern):
    return pattern + ".dat.gz"

def rm_timestamp_csv(pattern):
    return pattern + ".csv"

def process_future_cost(input, dry_run):
    # To concatenate the future cost files where tail -n +2 - outputs the lines read from the standard input (-) starting from the second line.
    cmd = '{ zcat %s/*future_cost*.gz | head -1 &&     find %s/*future_cost*.gz       -exec sh -c "zcat -q -c {} | tail -n +2 -" \;;       } | gzip - > %s/pdx_future_cost.dat.gz' %(input, input, input)
    print('DRY RUN:' + cmd)
    if dry_run is not True:
        ps = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ps.communicate()[0]
    return

def s3_folder():
    d = datetime.datetime.today().weekday()
    if d == 5:
        folder = "weekly_" + datetime.datetime.today().strftime('%Y%m%d')
    else:
        folder = "daily_" + datetime.datetime.today().strftime('%Y%m%d')    
    return folder

expected_filenames = [
    {"pattern": "pdx_product_hierarchy", "test": "1", "ext": "gz"},
    {"pattern": "pdx_product_attributes", "test": "1", "ext": "gz"},
    {"pattern": "pdx_product_cost", "test": "1", "ext": "gz"},
    {"pattern": "pdx_sales_data", "test": "1", "ext": "gz"},
    {"pattern": "pdx_item_store", "test": "1", "ext": "gz"},
    {"pattern": "pdx_supplier_id", "test": "1", "ext": "gz"},
    {"pattern": "pdx_supplier_name", "test": "1", "ext": "gz"},
    {"pattern": "pdx_location_hierarchy", "test": "1", "ext": "gz"},
    {"pattern": "pdx_loc_attributes", "test": "1", "ext": "gz"},
    {"pattern": "pdx_brand_name", "test": "1", "ext": "gz"},
    {"pattern": "category_lfg", "test": "1", "ext": "gz"},
    {"pattern": "class_lfg", "test": "1", "ext": "gz"},
    {"pattern": "subcategory_lfg", "test": "1", "ext": "gz"},
    {"pattern": "reference_asst", "test": "1", "ext": "gz"},
    {"pattern": "pdx_calendar_hierarchy", "test": "0+", "ext": "gz"},
    {"pattern": "price_type", "test": "0+", "ext": "gz"},   
    {"pattern": "future_cost", "test": "1+", "ext": "gz"},
    {"pattern": "category_changes", "test": "0+", "ext": "csv"},
    {"pattern": "class_changes", "test": "0+", "ext": "csv"},
    {"pattern": "subcategory_changes", "test": "0+", "ext": "csv"}

]

def process_pattern(expected_filename, remaining_filenames, key, bucket, dry_run):
    new_errors = []
    pattern = expected_filename.get('pattern')
    test = expected_filename.get('test')
    ext = expected_filename.get('ext')
    ### Add a '/' to the pattern to match the beginning of the last bit of path
    matches = [a for a in remaining_filenames if '/' + pattern in a]
    print("matches are : " ,matches)
    if test=="0+" and ext=="gz": 
        if len(matches)>0:
            for match in matches:
                    filename = rm_timestamp_gz(pattern)
                    object_name = bucket.lstrip('\"').rstrip('\"') + '/' + s3_folder() + '/' + 'staging'
                    dest = object_name + '/' + filename
                    cloud_store_upload(
                        input=match,
                        dest=dest,
                        dry_run=dry_run,
                        encryption_key=key)
                    print("Removing : " + match)
                    os.remove(match)
        else: 
            pass
    elif test=="0+" and ext=="csv":
        if len(matches)>0:
            for match in matches:
                    filename = rm_timestamp_csv(pattern)
                    object_name = bucket.lstrip('\"').rstrip('\"') + '/' + s3_folder() + '/' + 'staging'
                    dest = object_name + '/' + filename
                    cloud_store_upload(
                        input=match,
                        dest=dest,
                        dry_run=dry_run,
                        encryption_key=key)
                    print("Removing : " + match)
                    os.remove(match)
        else: 
            pass
    elif test=="1" and ext=="gz":
        if len(matches)==1:
            for match in matches:
                    filename = rm_timestamp_gz(pattern)
                    object_name = bucket.lstrip('\"').rstrip('\"') + '/' + s3_folder() + '/' + 'staging'
                    dest = object_name + '/' + filename
                    cloud_store_upload(
                        input=match,
                        dest=dest,
                        dry_run=dry_run,
                        encryption_key=key)
                    print("Removing : " + match)
                    os.remove(match)
        else:
            new_errors.append('ERROR: Test %s failed for pattern %s in %s'%(test, pattern,matches))
    elif test== "1+" and ext=="gz":
            costs_matches = [a for a in remaining_filenames if pattern in a]
            if len(costs_matches)>=1:
                dirpath = os.path.dirname(os.path.realpath(costs_matches[1])) # To get directory path for next function
                process_future_cost(
                    input=dirpath, 
                    dry_run=dry_run)
                remaining_filenames = [a for a in remaining_filenames if a not in costs_matches]
                filename = rm_timestamp_gz('pdx_' + pattern)
                object_name = bucket.lstrip('\"').rstrip('\"') + '/' + s3_folder() + '/' + 'staging'
                dest = object_name + '/' + filename
                input = dirpath + '/' + 'pdx_future_cost.dat.gz'
                cloud_store_upload(
                    input=input,
                    dest=dest,
                    dry_run=dry_run,
                    encryption_key=key)
                for f in costs_matches: # To Remove all matches of future_cost pattern we received from WFM
                    print("Removing : " + f)
                    os.remove(f)
                if dry_run is not True: os.remove(input) # To Remove pdx_future_cost.gz file
            else:
                new_errors.append('ERROR: Test %s failed for pattern %s in %s'%(test, pattern,costs_matches))
    else:
        pass
    remaining_filenames = [a for a in remaining_filenames if a not in matches]
    return remaining_filenames, new_errors


def cloud_store_upload_all(filenames, dest, encryption_key, dry_run):
    for filename in filenames:
        (head, tail) = os.path.split(filename)
        cloud_store_upload(filename, '%s/%s'%(dest, tail), dry_run, encryption_key)
    return


def main(incoming_dir, bucket, key, dry_run):
    errors = []
    incoming_dir = os.path.abspath(incoming_dir)
    # Note this is a recursive glob because some of the optional files may arrive in an "optional" subdirectory
    incoming_filenames = [a for a in glob.glob(rf"{incoming_dir}*/*", recursive=True) if not os.path.isdir(a)]
    dest = bucket.lstrip('\"').rstrip('\"') + '/' + s3_folder() + '/' + "to_pdx"
    archive_dest = bucket.lstrip('\"').rstrip('\"') + '/' + "to_pdx_archive"
    print('Incoming:' + '\n'.join(incoming_filenames))
    print('############### Backuping files: ###############')
    cloud_store_upload_all(
        filenames=incoming_filenames,
        dest=dest,
        encryption_key=key,
        dry_run=dry_run
    )
    cloud_store_upload_all(
        filenames=incoming_filenames,
        dest=archive_dest,
        encryption_key=key,
        dry_run=dry_run
    )
    print('############### Processing files: ###############')
    remaining_filenames = incoming_filenames
    for expected_filename in expected_filenames:
        remaining_filenames, new_errors = process_pattern(expected_filename, remaining_filenames, key, bucket, dry_run)
        if new_errors: 
            errors.append(new_errors)            

    if remaining_filenames or errors:
        print("ERRORS")
        print("Unrecognized files:\n" + "\n".join(remaining_filenames))
        print('\n'.join(''.join(ne) for ne in errors))
        exit(1)
    else:
        print('############### Staging Preprocessing has finished Successfully ###############')
        exit(0)



if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Process incoming files from WFM daily.')
    p.add_argument('--incoming-dir', metavar='DIR',
                   default="/data/lb_deployment/incoming",
                   help='Local destination of incoming SFTP files')
    p.add_argument('--key', metavar='KEY',
                   default="encryption-key-name",
                   help='s3 encryption key')
    p.add_argument('--bucket', metavar='URL',
                   default="s3://wfmp-pilot",
                   help='S3 destination for backup e.g. s3://wfmp-pilot')
    p.add_argument('--dry-run', action='store_true',
                   help='Print what would happen only')
    args = p.parse_args()
    main(incoming_dir=args.incoming_dir,
         bucket=args.bucket,
         key=args.key,
         dry_run=args.dry_run)

