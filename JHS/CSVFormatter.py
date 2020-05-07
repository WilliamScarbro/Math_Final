import csv
import glob
import os
import sys
from os import path

def main():
    input = sys.argv[1]
    out = sys.argv[2]
    clearOutput(out)
    with open(input, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        output(reader, out)

def clearOutput(out):
    files = glob.glob(out+'/*')
    for f in files:
        os.remove(f)

def output(reader, out):
    next(reader) #remove header line
    for row in reader:
        writeLine(row, out)

def writeLine(row, out):
    filename = out+'/'+row[1].replace(' ', '_')+'.csv'
    if not path.exists(filename):
        createFile(filename)
    with open(filename, 'a+', newline='') as csvfile:
        line = [row[0], row[3], row[4]]
        writer = csv.writer(csvfile)
        writer.writerow(line)


def createFile(filename):
    file = open(filename, "w+")
    file.write('date,confirmed_cases,deaths\n')


if __name__ == '__main__':
    main()
