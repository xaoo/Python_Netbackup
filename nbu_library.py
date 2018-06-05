import subprocess
import re

def run_process(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,shell=True)
    return p.stdout.read()#.decode('ascii','ignore')

def get_ret_table():

    CMD = ('bpretlevel','-U')
    rt = run_process(CMD).split()
    
    new_rt = {}
    i = 0
    for v in ret_list:
        if v == 'infinity':
            new_rt[ret_list[i-2]] = ret_list[i]
        if v in ('week', 'weeks', 'month', 'months', 'year', 'years'):
            new_rt[ret_list[i-3]] = ret_list[i-2]
        i+=1
    return new_rt

def get_client_info(clnt):
    
    CMD = ('bpgetconfig','-g',clnt,'-L')
    CMD2 = ('bpclntcmd','-hn', clnt)

    cip = run_process(CMD2)     
    cd = run_process(CMD)

    cd = re.split('= |\r\n',cd)
    list = ('NetBackup Client Platform',
            'NetBackup Client Protocol Level',
            'Product',
            'Version Name',
            'Version Number',
            'NetBackup Installation Path',
            'Client OS/Release')
    
    new_cd = {}
    i = 3
    for v in list:
        try:
            new_cd[v] = cd[i]
            i+=2
        except IndexError as ierr:
            new_cd['Data'] = 'No data fetched'      

    for x in cip.split():
        found = re.match('\d+\.\d+\.\d+\.\d+',x)
        if found:
            new_cd['Client IP'] = found.group()

    return new_cd

