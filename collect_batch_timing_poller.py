#!/usr/bin/env python3

############################################################
# Please note that starting this script will automatically #
# collect timings for all existing finished workflows.     #
############################################################

import argparse
import os
import sys
import logging
import time
import urllib.request
import urllib.error
import socket
import json
import tempfile
import shutil
from datetime import datetime

COMPATIBLE_WORKFLOWS = ['bootstrap_app', 'weekly', 'daily']
STATUS_SERVICE = '/status'
TIMEOUT = 1 * 60  # in seconds
PAYLOAD_WF_SUMMARY = '{ "get_only_roots": true, "get_root_summary": true }'
PAYLOAD_HISTORY_TPL = '{ "workflow_root_uid": "%s", "get_io": true }'

DEFAULT_DATA_ROOT = os.path.curdir
DEFAULT_TRANSPORT = 'http://localhost:55183'
DEFAULT_POLL_TIME = 5 * 60  # in seconds

WF_TIMESTAMAP_FORMAT = '%Y-%m-%d %H:%M:%S.%f %Z'


def retrieve_workflow_instances(transport):
    logging.info('Retrieving the list of workflow instances...')
    request = urllib.request.Request('%s%s' % (transport, STATUS_SERVICE),
                                     data=bytes(PAYLOAD_WF_SUMMARY.encode('utf-8')),
                                     headers={'content-type': 'application/json', 'accept': 'application/json'})
    content = urllib.request.urlopen(request, None, TIMEOUT).read()
    response = json.loads(content)
    return response.get('workflow')


def process_workflows(wfs):
    wfs_obj = {}
    for workflow in wfs:
        name = workflow.get('name')
        instances = workflow.get('instance')
        for instance in instances:
            root_id = instance.get('root_id')
            global_state = instance.get('global_state')
            timestamp = instance.get('timestamp')
            wfs_obj[root_id] = {
                'name': name,
                'state': global_state,
                'timestamp': datetime.strptime(timestamp, WF_TIMESTAMAP_FORMAT)
            }
    return wfs_obj


def process_instances(cur, prev):
    prev_roots = prev.keys()
    cur_roots = cur.keys()
    finished_roots = []
    for root in cur_roots:
        if root not in prev_roots:
            logging.info('New workflow "%s" has been found with a root %s' % (cur[root]['name'], root))
        if cur[root]['state'] == 'END':
            if root not in prev_roots or prev[root]['state'] != 'END':
                finished_roots.append(root)
    return finished_roots


def retrieve_json_status(root, transport):
    try:
        request = urllib.request.Request('%s%s' % (transport, STATUS_SERVICE),
                                         data=bytes((PAYLOAD_HISTORY_TPL % root).encode('utf-8')),
                                         headers={'content-type': 'application/json', 'accept': 'application/json'})
        content = urllib.request.urlopen(request, None, TIMEOUT).read()
        return json.loads(content)
    except socket.timeout:
        logging.error('Request has timed out!')
        return None
    except ConnectionRefusedError:
        logging.error('Connection refused!')
        return None


def collect_and_upload_batch_timings(roots, cur_wfs, pathname, data_root, transport):
    for root in roots:
        current_wf = cur_wfs[root]
        wf_name = current_wf['name']
        if wf_name not in COMPATIBLE_WORKFLOWS:
            logging.warning('Incompatible workflow "%s" has finished. Skipping...')
            continue

        logging.info('Retrieving the full status for workflow "%s" with a root "%s"...' % (wf_name, root))
        json_status = retrieve_json_status(root, transport)
        if json_status is None:
            logging.error('Can\'t collect timings for the workflow "%s" with a root %s' % (wf_name, root))
            continue

        tmp_dir = tempfile.mkdtemp()
        status_file = '%s%sstatus.json' % (tmp_dir, os.path.sep)
        file = open(status_file, 'w')
        json.dump(json_status, file)
        file.close()

        logging.info('Collecting batch timings for the workflow "%s" with root id %s' % (wf_name, root))
        os.system('%s/collect_batch_timings.py --type %s --root %s --path %s %s'
                  % (pathname, wf_name, root, '%s/timings' % tmp_dir, status_file))

        dest = '%s/timings/%s-wf_%s-%s' % (data_root, current_wf['timestamp'].strftime('%Y%m%d_%H%M%S'), wf_name, root)
        logging.info('Uploading batch timings for a workflow "%s" with root id %s to %s' % (wf_name, root, dest))
        os.system('cloud-store upload -i %s/timings/ %s/ --recursive --progress' % (tmp_dir, dest))

        shutil.rmtree(tmp_dir)


def main():
    current_workflows = {}

    parser = argparse.ArgumentParser(description='Poller script to identify when a batch is finished and collect its '
                                                 'timings.')
    parser.add_argument('--data-root',
                        default=DEFAULT_DATA_ROOT,
                        type=str,
                        help='Path to store resulting timings. Default: current directory.')
    parser.add_argument('--transport',
                        default=DEFAULT_TRANSPORT,
                        type=str,
                        help='Transport that is used to reach workflow %s service. Default: %s'
                             % (STATUS_SERVICE, DEFAULT_TRANSPORT))
    parser.add_argument('--poll-time',
                        default=DEFAULT_POLL_TIME,
                        type=int,
                        help='How often to poll for workflow status. Default: %s seconds.' % DEFAULT_POLL_TIME)
    args = parser.parse_args()

    logging.basicConfig(level='INFO', format='%(levelname)s - %(message)s')

    pathname = os.path.dirname(sys.argv[0])

    if '--data-root' not in sys.argv:
        logging.warning('--data-root is not explicitly specified, using "%s" as the default value.' % DEFAULT_DATA_ROOT)

    if '--transport' not in sys.argv:
        logging.warning('--transport is not explicitly specified, using "%s" as the default value.' % DEFAULT_TRANSPORT)

    if '--poll-time' not in sys.argv:
        logging.warning('--poll-time is not explicitly specified, using "%s" as the default value.' % DEFAULT_POLL_TIME)

    while True:
        logging.info('/------------------ (BEGIN POLL CYCLE) ------------------\\')

        try:
            workflows = retrieve_workflow_instances(args.transport)
            previous_workflows = current_workflows
            current_workflows = process_workflows(workflows)
            finished_roots = process_instances(current_workflows, previous_workflows)
            collect_and_upload_batch_timings(finished_roots, current_workflows, pathname, args.data_root,
                                             args.transport)
        except socket.timeout:
            logging.error('Request has timed out!')
        except ConnectionRefusedError:
            logging.error('Connection refused!')
        except urllib.error.HTTPError as e:
            logging.error(e)

        logging.info('\\------------------- (END POLL CYCLE) -------------------/')
        time.sleep(args.poll_time)


if __name__ == '__main__':
    main()

