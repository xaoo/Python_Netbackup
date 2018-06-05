
rt subprocess
import json
import sys
import pybpdbjobs
import io

try:
    to_unicode = unicode
except NameError:
    to_unicode = str


def run_process(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,shell=True)
    return p.stdout.read().decode('ascii','ignore')


def write_to_file(write_file,write_mode,data):
    with io.open(write_file,mode=write_mode,encoding='utf-8') as outfile:
            outfile.write(to_unicode(data))


def main(jsonfile):
    '''
    Generates a json file with the output of bpdbjobs NBU native cmd.

    It accepts absolute path as a param
    It can be runned from REPL and it will ask for json file absolute path
    '''
    BPDBJOBS = ['bpdbjobs.exe','-report','-all_columns','-mastertime']

    write_to_file(jsonfile,'w','[')
    parser = pybpdbjobs.Parser()
    runned_BPDBJOBS = run_process(BPDBJOBS).split('\n')
    lenght = len(runned_BPDBJOBS)-2
    i = 0

    for input_line in runned_BPDBJOBS:
        jobobj = parser.process_line(input_line)
        if jobobj:
            if i == lenght:
                str_ = json.dumps(jobobj,sort_keys=True,indent=4,
                           default=parser.json_serializer,ensure_ascii=False)
                write_to_file(jsonfile,'a',str_)
            else:
                str_ = json.dumps(jobobj,sort_keys=True,indent=4,
                           default=parser.json_serializer,ensure_ascii=False)+','
                write_to_file(jsonfile,'a',str_)
        i += 1

    write_to_file(jsonfile,'a',']')

if __name__ == '__main__':
    try:
        main(sys.argv[1])
    except IndexError as ierr:
        print('Tell me where to save the json file:')
        arg1 = raw_input('> ')
        main(arg1)

