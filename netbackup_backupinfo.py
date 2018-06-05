import json
import csv
import re
import sys
import datetime
import nbu_library as nblib


def populate_client_data(client_data_dict,clnt):
    try:
        backup_client_os = client_data_dict[clnt]['Data']
        backup_tool_version = client_data_dict[clnt]['Data']
    except KeyError as kerr:
        backup_client_os = client_data_dict[clnt]['Client OS/Release']
        backup_tool_version = client_data_dict[clnt]['NetBackup Client Protocol Level']
    try:
        backup_client_ip = client_data_dict[clnt]['Client IP']
    except KeyError as kerr:
        backup_client_ip = client_data_dict[clnt]['Data']
                    
    return backup_client_os, backup_tool_version, backup_client_ip


def read_json(abspath):
    data = {}
    try:   
        with open(abspath) as fp:
            data = json.load(fp)
        return data
    except IOError as ierr:
        return data


def write_json(clnt_dict,abspath):
    with open(abspath,'w') as fp:
        data = json.dump(clnt_dict,fp)
            

def generate_backup_info_csv(jsonfile,csvfile,clnts_info_file):
    '''
    Receives json formated file, arranged with pybpdbjobs module
    Exports a csvfile with a specific data set

    Cache every clients data to a json file.

    argv[1]:
        is the json file to read from, dumped from pybpdbjobs lib
    argv[2]:
        is the csv file to export data to.
    argv[3]:
        a json file containing clinets data, if
        the file doesnt exist, create a new one.
        Acts as a cache, so populate_client_data() wont
        generate new data every line.
    '''

    pf = json.loads(open(jsonfile).read())
    csvfile = open(csvfile,'wb')

    header = ['backup_id', 'parent_job_id', 'backup_job', 'backup_status','backup_state','backup_tries','backup_trycount','backup_client',
              'backup_files','backup_client_ip','backup_client_os','backup_tool_version','master_server','media_server',
              'backup_start','backup_end','backup_policy','backup_type','backup_policy_type','backup_policy_subtype',
              'backup_duration','retention_period','retention_period_end','backup_size_brutto','backup_size_netto']

    writer = csv.DictWriter(csvfile, fieldnames=header)
    writer.writeheader()

    ret_table = nblib.get_ret_table()
    now_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    #collet clients info if any
    clnt_dict = read_json(clnts_info_file)
    
    for bckjob in pf:
        if bckjob['type'] != 'Backup':
            continue
        
        backup_id = bckjob['id']
        backup_job = bckjob['backupid']
        backup_parent_id = bckjob['parentjob']
        backup_status = bckjob['status']
        backup_state = bckjob['state']

        backup_tries = bckjob['jobtry']
        backup_trycount = bckjob['trycount']

        backup_client = bckjob['client']

        bf = []
        files = bckjob['filelist']
        sep = "#"
        for f in files:
            f = f.encode("utf-8")
            bf.append(f)
        backup_files = sep.join(bf)

        #print('Getting {} info'.format(backup_client))
              
        if backup_client in clnt_dict:
            backup_client_os, backup_tool_version, backup_client_ip = populate_client_data(clnt_dict,backup_client)
        else:
            clnt = nblib.get_client_info(backup_client)
            clnt_dict[backup_client] = clnt
            backup_client_os, backup_tool_version, backup_client_ip = populate_client_data(clnt_dict,backup_client)
            
        master_server = bckjob['master_server']
        media_server = bckjob['server']
        backup_start = datetime.datetime.fromtimestamp(int(bckjob['start'])).strftime("%Y-%m-%d %I:%M:%S")
        backup_end = datetime.datetime.fromtimestamp(int(bckjob['end'])).strftime("%Y-%m-%d %I:%M:%S")
        backup_policy = bckjob['policyname']

        backup_type = bckjob['type']
        backup_policy_type = bckjob['policytype']
        backup_policy_subtype = bckjob['subtype']

        backup_duration = bckjob['elapsed']
        retention_period = ret_table[bckjob['retention_period']]
        retention_period_end = bckjob['retention_units'] #CAN BE CALCULATED bck start + ret_period
        backup_size_brutto = bckjob['kbytes']

        dedup = bckjob['tries'] #GET
        report_date = now_date

        backup_size_netto = int()
        for k,v in dedup.iteritems():
            patern = re.compile(r"dedup:\s\d*.\d+")
            for v in v['statuslines']:
                s = re.search(patern,v)
                if s is not None:
                    s = float(s.group().split(':')[1].strip())
                    if backup_size_brutto:
                        backup_size_brutto = int(backup_size_brutto)
                        result = backup_size_brutto - (backup_size_brutto * s/100)
                        backup_size_netto = round(result,2)

        writer.writerow({
                'backup_id':backup_id,
                'parent_job_id':backup_parent_id,
                'backup_job':backup_job,
                'backup_status':backup_status,
                'backup_state':backup_state,
                'backup_tries':backup_tries,
                'backup_trycount':backup_trycount,
                'backup_client':backup_client,
                'backup_files':backup_files,
                'backup_client_ip':backup_client_ip,
                'backup_client_os':backup_client_os,
                'backup_tool_version':backup_tool_version,
                'master_server':master_server,
                'media_server':media_server,
                'backup_start':backup_start,
                'backup_end':backup_end,
                'backup_policy':backup_policy,
                'backup_type':backup_type,
                'backup_policy_type':backup_policy_type,
                'backup_policy_subtype':backup_policy_subtype,
                'backup_duration':backup_duration,
                'retention_period':retention_period,
                'retention_period_end':retention_period_end,
                'backup_size_brutto':backup_size_brutto,
                'backup_size_netto':backup_size_netto
        })

    csvfile.close()
    write_json(clnt_dict,clnts_info_file)
    

def main(jsonfile,csvfile,clnts_info_file):
    generate_backup_info_csv(jsonfile,csvfile,clnts_info_file)


if __name__ == '__main__':
    try:
        main(sys.argv[1],sys.argv[2],sys.argv[3])
    except IndexError as ierr:
        print('Tell me the path to json file, need to read it:')
        arg1 = raw_input('> ')
        print('Tell me the path to csv file, need to write to it:')
        arg2 = raw_input('> ')
        print('Tell me the path where to read or write clients info:')
        arg3 = raw_input('> ')
        
        main(arg1,arg2,arg3)
