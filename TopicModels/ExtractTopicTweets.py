## code by Thomas Lujan
## MATH 435 Final Project
## code to extract top topics from text file and obtain csv metadata
## state information will be manually swapped out per use.

import pandas as pd

def main():
    topic = 93
    data = loadTweets()
    docs = loadDocs(topic)
    processDocs(data, docs)

def loadTweets():
    data = pd.read_csv('AllTweets/new_york.csv.csv')
    data.columns = ['id','state','date','text']
    return data

def loadDocs(topic):
    file = open('new_york.txt','r')
    lines = file.readlines()
    docNumbers = []
    for line in lines:
        tokens = line.split(',')
        docNum = int(tokens[0])
        topicNum = tokens[1].replace('[','').replace(']','')
        topicNums = []
        for n in topicNum.split(' '):
            if (len(n) > 0):
              topicNums.append(int(n))
        if topic in topicNums:
            docNumbers.append(docNum)
    return docNumbers

def processDocs(data, docs):
    lines = []
    for doc in docs:
        row = data.iloc[doc+1]
        lines.append(str(row['id'])+','+str(row['state'])+','+str(row['date'])+','+str(row['text'])+'\n')

    file = open('NYFiltered.csv', 'w')
    file.writelines(lines)
    file.close

if __name__ == '__main__':
    main()
