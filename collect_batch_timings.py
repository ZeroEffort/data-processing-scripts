#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime
import json
import re
import itertools
import os

parser = argparse.ArgumentParser(description='Program extracts timings for a given batch from `lb workflow status '
                                             '--json` output.')
parser.add_argument('json', metavar='JSON_FILE', help='Path to file with workflow status output in JSON format.')
parser.add_argument('--type', help='Type of a batch.', required=True, choices=['daily', 'weekly', 'bootstrap_app'])
parser.add_argument('--root', help='Workflow root. If not provided, the latest batch is used.', type=int)
parser.add_argument('--path', help='Path to store the result CSV file. Default to current directory.',
                    default=os.path.curdir)
args = parser.parse_args()

FORMAT = '%Y-%m-%d %H:%M:%S.%f'


def steps_sort(x):
    if x['Scope'] == "APP":
        N = '0'
    elif x['Scope'] == "stage":
        N = '1'
    elif x['Scope'] == "admin":
        N = '2'
    elif x['Scope'] in {"ADM","SEC"}:
        N = '3'
    elif x['Scope'] == "data":
        N = '4'
    elif x['Scope'] in {"MG_IS","MG_PS","PL_IS","PL_PS"}:
        N = '3'
    elif x['Scope'] == "proxy":
        N = '4'
    elif x['Scope'] == "main_sections":
        N = '5'
    return N + '_' + x['timestamp']


def duration(start, end):
    return datetime.strptime(end, '%Y-%m-%d %H:%M:%S.%f') - datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f')


def remove_tz(dt_str):
    return dt_str.replace(' UTC', '')


def extract_timings(name):
    def worker(content, root, path):
        def completed_instance_filter(inst):
            try:
                instance_processes = inst.get('process')
                root_process = next(x for x in instance_processes if x.get('root'))
                return root_process.get('state') == 'All Children Terminated'
            except StopIteration:
                return False

        steps = []
        workflows = content.get('workflow')
        try:
            batch = next((x for x in workflows if x['name'] == name), None)
            instances = batch.get('instance')
            completed_instances = sorted([x for x in instances if completed_instance_filter(x)],
                                         key=lambda x: x.get('timestamp'), reverse=True)
            if root is None:
                instance = completed_instances[0]
            else:
                instance = next(x for x in completed_instances if x.get('root_id') == root)
            processes = instance.get('process')
            log_messages = list([x for x in processes if x.get('process_name').startswith('lb.Log')])
        except TypeError:
            raise AssertionError('Provided status file doesn\'t contain information about %s.' % name)
        except StopIteration:
            raise AssertionError('There is no information about workflow with root id %s.' % root)

        for message in log_messages:
            inputs = message.get('input')
            msg_input = list([x for x in inputs if x.get('key') == 'msg'])[0]
            step_info = {}
            msg = msg_input.get('value')[0]
            accepted_message = re.search(r"COMPLETE|START", msg)
            if accepted_message is not None:
                splitted_msg = msg.split()
                type_batch = msg.split(' ')[0]
                matches_daily=re.search(r"template_instantiation_id",msg.split(' ',1)[1])
                blocks_batches_tasks=['gbm.planning.ExecutleLogiQLBlocksMasterPartitioned', 'gbm.planning.ExecutleLogiQLQueryMasterPartitioned','gbm.planning.GetWorkbookIdsByTemplate']
                main_sections=["BOOTSTRAP_STAGE","BOOTSTRAP_MASTER","BOOTSTRAP_MASTER_ADMIN","WEEKLY_STAGE","WEEKLY_MASTER","WEEKLY_MASTER_ADMIN","DAILY_STAGE","DAILY_MASTER","DAILY_MASTER_ADMIN"]
                if msg.split(' ',1)[1].split()[0] == name.upper():
                    batch_start = remove_tz(message.get('begins_timestamp'))
                    batch_end = remove_tz(message.get('terminates_timestamp'))
                try:
                    if msg.split(' ',1)[1].split()[0] not in blocks_batches_tasks and matches_daily is None:
                        if msg.split(' ',1)[1].split()[0] not in main_sections:
                            if splitted_msg[-1].replace('.', '') == 'data' and splitted_msg[-2].replace('.', '') == 'proxy':
                                    scope=splitted_msg[-2].replace('.', '')
                            elif splitted_msg[-1].replace('.', '') in ['data','admin','stage','proxy','ADM','SEC','MG_IS','MG_PS','PL_IS','PL_PS']:
                                    scope=splitted_msg[-1].replace('.', '')
                            elif splitted_msg[-1] in "all.":
                                    scope=splitted_msg[-2].replace('.', '')
                            else:
                                    scope='APP'
                        else:
                            scope="main_sections"
                        step_info['Scope'] = scope
                        step_info['Task'] = msg.split(' ',1)[1]
                        step_info['type'] = type_batch
                        if type_batch == 'START':
                            step_info['timestamp'] = remove_tz(message.get('begins_timestamp'))
                        elif type_batch == 'COMPLETE':
                                step_info['timestamp'] = remove_tz(message.get('terminates_timestamp'))
                        steps.append(step_info)
                except AttributeError:
                    if 'START' in msg:
                        batch_start = remove_tz(message.get('begins_timestamp'))
                    if 'COMPLETE' in msg:
                        batch_end = remove_tz(message.get('terminates_timestamp'))

        steps.sort(key=steps_sort)
        steps.sort(key=lambda x: x['Task'])
        grouped_steps = [(k, list(g)) for k, g in itertools.groupby(steps, key=lambda x: (x['Task'],x['Scope']))]
        output = []

        for key, group in grouped_steps:
            if len(group) == 2 and group[0].get('type')=="START" and group[1].get('type')=="COMPLETE":
                row = {'task' : key[0], 'scope': key[1]}
                step_start = None
                step_finish = None
                for step in group:
                    if step['type'] == 'START':
                        step_start = step['timestamp']
                    elif step['type'] == 'COMPLETE':
                            step_finish = step['timestamp']
                row['start'] = step_start
                row['end'] = step_finish
                row['duration'] = duration(step_start, step_finish)
                output.append(row)

        if not os.path.exists(path):
            os.makedirs(path)

        out_file = open('%s%s_%s_timings.csv' % (path + os.path.sep, name,
                                                 datetime.strptime(batch_start, FORMAT).strftime("%Y%m%d")), 'w')
        writer = csv.DictWriter(out_file, delimiter='|', fieldnames=['task', 'scope', 'start', 'end', 'duration'])
        writer.writeheader()
        for row in output:
            writer.writerow(row)
        writer.writerow({
            'task': 'ALL',
            'scope': name,
            'start': batch_start,
            'end': batch_end,
            'duration': duration(batch_start, batch_end)
        })
    return worker


# If you're updating this, please update COMPATIBLE_WORKFLOWS in collect_batch_timings_poller.py as well.
matcher = {
    'weekly': extract_timings('weekly'),
    'daily': extract_timings('daily'),
    'bootstrap_app': extract_timings('bootstrap_app')
}

f = open(args.json, 'r')
contents = json.load(f)
f.close()

matcher[args.type](contents, args.root, os.path.abspath(args.path))

