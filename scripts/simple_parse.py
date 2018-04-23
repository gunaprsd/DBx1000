dicts = {}
for i in xrange(0, 16):
    dicts[i] = []

counter = 0
for line in open('temp.txt', 'r'):
    dicts[counter].append(line.strip())
    counter += 1
    counter %= 16

for i in dicts:
    s = ""
    for j in range(0, len(dicts[i])):
        s += dicts[i][j]
        s += ", "
    print s
