import re
import sys

keys = ['Num Vertices', 'Num Edges', 'Cross-Core Accesses', 'Min Data Degree', 'Max Data Degree', 'Min Cross Data Degree', 'Max Cross Data Degree']
keys2 = ['First Pass', 'Second Pass', 'Third Pass', 'Partition']
data = []

def find_partition_summary(s, r):
    rext = r
    while(len(rext) < 30):
        rext += ' '
    rext += ': '
    m = re.findall('(?<=' + rext + ')[0-9]+\.*[0-9]*', s)
    m = map(lambda x: x.strip(), m)
    return m

def find_execution_summary(s, key):
    rext = key
    while(len(rext) < 25):
        rext += ' '
    rext += ' :: total:'
    m = re.findall('(?<=' + rext + ')\ +[0-9]+\.*[0-9]*', s)
    m = map(lambda x: x.strip(), m)
    return m

def pretty_print():
    num_cols = len(data)
    num_rows = len(data[0])
    for j in xrange(num_rows):
        row = ''
        for i in xrange(0, num_cols):
            row += data[i][j]
            row += ', '
        print row


def parse(fname):
    f = open(fname, 'r')
    s = f.read()
    for key in keys:
        col = find_partition_summary(s, key)
        data.append(col)
    for key in keys2:
        col = find_execution_summary(s, key)
        data.append(col)
    pretty_print()

if __name__ == "__main__":
    filename = sys.argv[1]
    parse(filename)
