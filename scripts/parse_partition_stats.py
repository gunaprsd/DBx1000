import re
import sys

keys = ['Num-Vertices', 'Num-Edges', 'Pre-Total-Cross-Core-Access', 'Pre-Min-Cross-Core-Access', 'Pre-Max-Cross-Core-Access', 'Post-Total-Cross-Core-Access', 'Post-Min-Cross-Core-Access', 'Post-Max-Cross-Core-Access', 'Min-Cluster-Size', 'Max-Cluster-Size', 'First-Pass-Duration', 'Second-Pass-Duration', 'Third-Pass-Duration', 'Partition-Duration']
data = []

def find_value_for_key(string, rgx):
    rext = rgx
    while(len(rext) < 30):
        rext += ' '
    rext += ': '
    m = re.findall('(?<=' + rext + ')\ *[0-9]+\.*[0-9]*', string)
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
    sdata = f.read()
    for key in keys:
        col = find_value_for_key(sdata, key)
        data.append(col)
    pretty_print()

if __name__ == "__main__":
    filename = sys.argv[1]
    parse(filename)
