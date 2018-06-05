#----------------------------------------------
# Name:           export_NB_cfg_to_xml
# Version:        0.0.1
# Start date:     05.02.2018
# Release date:   05.02.2018
# Description:    this tool was converted from PowerShell
#				  its gathering all cfg data from a NB Server
#
# Author:         George Dicu
# Department:     Cloud, Backup
#----------------------------------------------

import xml.etree.cElementTree as ET
import os
import subprocess
import re
import platform

def run_command(cmd):
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT, shell=True)
    return iter(p.stdout.readline, '')

def get_policy_det(plc):
    cmd = "bppllist %s -U" % (plc)
    policy = []
    for line in run_command(cmd):
        policy += line.strip().split(',')
    return policy #[2:len(policy)-1]

def isActive(plc):
    active = [string for string in plc if re.match("^Active",string)]
    active = active[0].split()[1]
    if active=='yes':
        return True
    else:
        return False

def getSchedIndex(array):
    return [i for i,x in enumerate(array) if re.match("^Schedule",x)]

#return int data with 1st Include element from a policy
def getslIndex(array):
    return [i for i,x in enumerate(array) if re.match("Include",x)][0]

def getMatched(array,match):
    return [x for i,x in enumerate(array) if re.match(match,x)]

def retValue(list):
    for l in list:
        l = l.split(':',1)
        value = l[1]
        value = value.strip()
    return value
    
def retValue2(str):
        return str.split(':',1)[1].strip()
    
def raw2dict(plc):
    d = {}
    for p in plc[2:len(plc)-1]:
        split = p.split(':',1)
        
        if len(p) >= 2:
            name = split[0]
            value = split[1]
            d[name] = (value)
        elif len(p) <= 0:
            continue
        else:
            name = split[0]
            d[name] = ('NULL')
    return d

cmd = "bppllist"
policies = []
for line in run_command(cmd):
    policies += line.split()

stdPolicy = ['Active','Application Defined','Application Discovery','ASC Application and attributes',
             'Backup network drvs','Block Incremental','Checkpoint','Client Encrypt','Client List Type',
             'Collect BMR info','Collect TIR info','Data Classification','Database Backup Share Arguments',
             'Disaster Recovery','Discovery Lifetime','Effective date','File Restore Raw','Granular Restore Info',
             'Ignore Client Direct','Keyword','Max Jobs','Mult. Data Streams','Optimized Backup',
             'Policy Name','Policy Priority','Policy Type','Residence','Residence is Storage Lifecycle Policy',
             'Selection List Type','Server Group','Use Accelerator','Follow NFS Mounts','Client Compress','Volume Pool']

stdOracle = ['Oracle Backup Archived Redo Log Arguments','Oracle Backup Archived Redo Log File Name Format',
             'Oracle Backup Control File Name Format','Oracle Backup Data File Arguments','Oracle Backup Data File Name Format',
             'Oracle Backup Fast Recovery Area File Name Format','Oracle Backup Set ID']

stdSchedule = ["Calendar sched","Checksum Change Detection","Daily Windows","Fail on Error","Maximum MPX","Number Copies",
               "PFI Recovery","Residence","Residence is Storage Lifecycle Policy","Retention Level","Server Group",
               "Synthetic","Type","Volume Pool"]
"""
Going into each policy and get standard details
Also start xml build
"""
xmlroot = ET.Element("NetBackupStructure")
xmlpolicies = ET.SubElement(xmlroot, "Policies")
print 'If you have policies with query, pls run the script on Master Server'
for p in policies:

    #print 'Going trought '+p+' policy'
    #return all details about a policy in poldet
    poldet = get_policy_det(p)
    
    xmlpolicy = ET.SubElement(xmlpolicies,"Policy")
    xmlpolicy.attrib["Name"] = p
    
    xmlopt = ET.SubElement(xmlpolicy,"Options")
    
    '''
    since some of the stdPolciy can be found multiple times in the policy
    we need to split the policy details array, poldet,from the 1'st element
    until the first
    '''
    schedindex = getSchedIndex(poldet)
    
    for atr in stdPolicy:
        xmlatr = ET.SubElement(xmlopt,atr.replace(" ",'_'))        
        match = getMatched(poldet[0:schedindex[0]],atr+':')
        #if you cannot find stdPolicy in poldet, save null to the xml data!
        match = retValue2(match[0]) if match else 'null'
        xmlatr.text = match
        
    if retValue2(poldet[4])=='Oracle':
        for atr in stdOracle:
            xmlatr = ET.SubElement(xmlopt,atr.replace(" ",'_'))    
            match = getMatched(poldet[0:schedindex[0]],atr+':')
            #if you cannot find stdPolicy in poldet, save null to the xml data!
            match = retValue2(match[0]) if match else 'null'
            xmlatr.text = match
            
    '''
    getting all clients for each policy
        !!!CHECK IF SUITABLE FOR EACH TYPE OF POLICY!!!
    '''
    for c in run_command('bpplclients '+p+' -noheader'):
        c = c.split()
        xmlclnt = ET.SubElement(xmlpolicy,"Client")
        xmlclnt.attrib["Architecture"] = c[0]
        xmlclnt.attrib["OS"] = c[1]
        xmlclnt.attrib["Name"] = c[2]
        
    '''
    Getting clients for vmware query policy!
    check 1st if the policy is a vmware type
    2nd check is if it has a query based client
    '''
    query=retValue(getMatched(poldet,'Include:'))
    if retValue2(poldet[4])=='VMware' and 'vmware:/?' in query:
        print 'This backup server has vmware with query, if this is not the master server, this script would get stuck'
        for c in run_command('nbdiscover -noxmloutput -includedonly '+"'"+query+"'"):
           xmlclnt = ET.SubElement(xmlpolicy,"Client")
           xmlclnt.attrib["Name"] = c

    '''
    getting selection list, sl, aka Include from the policies
    there are also Include data in Schedules
    but we do that later
    '''
    xmlsl = ET.SubElement(xmlpolicy,"Selection_List")
    slIndex = getslIndex(poldet)
    for i,sl in enumerate(poldet[slIndex:schedindex[0]-1]):
        if i==0:
            xmll = ET.SubElement(xmlsl,"Selection")
            x = retValue2(sl).decode('utf8')
            xmll.text = x
        else:
            xmll = ET.SubElement(xmlsl,"Selection")
            x = sl.strip().decode('utf8')
            xmll.text = x
            
    '''
    getting all schedules from policies
    we break policy data, poldet,
    in 1st and last array element for each schedule, schedindex,
    this way it`s easier to samve them saparately 
    '''
    xmlschs = ET.SubElement(xmlpolicy,"Schedules")
    for i in range(len(schedindex)-1):
        xmlsch = ET.SubElement(xmlschs,"Schedule") 
        xmlsch.attrib["Name"] = retValue2(poldet[schedindex[i]])
        
        for sch in stdSchedule:
            xmlsch2 = ET.SubElement(xmlsch,sch.replace(" ",'_'))
            match = getMatched(poldet[schedindex[i]:schedindex[i+1]-1],sch+':')
            #if you cannot find stdSchedule in poldet, save null to the xml data!
            match = retValue2(match[0]) if match else 'null'
            xmlsch2.text = match
    
tree = ET.ElementTree(xmlroot)
system=platform.system()

if system=='Windows':
    path='C:\\temp\\test2.xml'
    print 'File was saved in '+path
    tree.write(path,encoding='UTF-8',method='xml',xml_declaration=True)
else:
    path='/tmp/test.xml'
    print 'File was saved in '+path
    tree.write(path)
