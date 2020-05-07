import pandas as pd
import re
import glob

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation as LDA

def main():
    data = loadData()
    data = preprocess(data)
    print(data.head())
    process(data)

def loadData():
    filenames = glob.glob(r'AllTweets/colorado.csv')

    dfs = []
    count = 1
    for file in filenames:
        print(file)
        if file == r'AllTweets/california.csv':
            continue
        df_chunks = []
        chunks = pd.read_csv(file, chunksize=100000)
        for chunk in chunks:
            chunk.columns = ['id','state','date','text']
            chunk = chunk.drop(columns=['id','state','date']).dropna()
            df_chunks.append(chunk)
        dfs.append(pd.concat(df_chunks))
        count -= 5
        if count <= 0:
            break
    data = pd.concat(dfs)

    return data

def preprocess(data):
    # remove all non words except space
    data['text'] = data['text'].map(lambda x: re.sub('[^\\w\\s]', '', str(x)))
    # to lower case
    data['text'] = data['text'].map(lambda x: x.lower())
    # remove stopwords & stem
    stop_words = set(stopwords.words('english'))
    data['text'] = data['text'].map(lambda x: ' '.join([w for w in word_tokenize(x) if not w in stop_words]))
    ps = PorterStemmer()
    data['text'] = data['text'].map(lambda x: ' '.join(ps.stem(w) for w in word_tokenize(x)))

    return data

def process(data):
    # init countVectorizer
    count_vectorizer = CountVectorizer(stop_words='english')
    count_data = count_vectorizer.fit_transform(data['text'])

    # number of topics
    number_topics = 100
    number_words = 10

    # lda model
    lda = LDA(n_components=number_topics, n_jobs=-1)
    lda.fit(count_data)

    print("Topics found via LDA:")
    printTopics(lda, count_vectorizer, number_words)

    doc_topic = lda.transform(count_data)
    file = open('Colorado.txt','a')
    for n in range(doc_topic.shape[0]):
        topic_most_pr = doc_topic[n].argmax()
        file.write("doc: {} topic: {}".format(n, topic_most_pr))
    file.close()

def printTopics(model, count_vectorizer, n_top_words):
    words = count_vectorizer.get_feature_names()
    for topic_idx, topic in enumerate(model.components_):
        print("\nTopic %d:" % topic_idx)
        print(" ".join([words[i] for i in topic.argsort()[:-n_top_words - 1:-1]]))

if __name__ == '__main__':
    main()
