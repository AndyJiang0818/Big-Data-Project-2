import numpy as np
from pyspark import SparkContext

vocab = {}


def build_vocabulary(value):
    if value not in vocab:
        vocab[value] = len(vocab) + 1


def build_matrix(indices, size):
    vec = np.zeros(size)
    for id, value in indices:
        vec[id] = value
    return vec


def final_matrix(vec_list):
    matrix = []
    for vec in vec_list:
        matrix.append(vec)
    return np.array(matrix)


sc = SparkContext(appName='tfidf')
df = sc.textFile('project2_test.txt')
total_document = df.count()

# convert text into (docID,[word1, word2, word3 ... ])
key_Text = df.map(lambda x: (x.split()[0], x.split()[1:]))

# get the number of word of each count (docId, word count)
doc_word_count = key_Text.map(lambda x: (x[0], len(x[1])))
print(doc_word_count.collect())
key_word_list = key_Text.flatMap(
    lambda x: [(y, 1) for y in x[1]]).reduceByKey(lambda x, y: x+y)

tatal_vocab_count = key_word_list.count()

# build the vocabulary, convert word to word
word_sorted = key_word_list.top(tatal_vocab_count, key=lambda x: x[1])
word_order = sc.parallelize(range(tatal_vocab_count))
dictionary = word_order.map(lambda x: (word_sorted[x][0], x))

# convert  key_test into ((doc id, token),tf)
key_count = key_Text.flatMap(
    lambda x: [((x[0], y), 1) for y in x[1]]).reduceByKey(lambda x, y: x+y)

# (docId, ((token,tf),doc count))
tf = key_count.map(lambda x: (x[0][0], (x[0][1], x[1]))).join(
    doc_word_count).map(lambda x: (x[1][0][0], (x[0], x[1][0][1]/x[1][1])))

# convert key token into  (token,(doc id,tf)) format
# tf = key_count.map(lambda x:(x[0][1],(x[0][0],x[1]/tatal_vocab_count)))
# convert key token from  (token, (doc id, tf)) => (token, (doc id, tf, 1))
key_converted_token = tf.map(lambda x: (x[0], (x[1][0], x[1][1], 1)))

# extract the token and the number of counter of 1
# convert from (token,(doc id, tf, 1)) => (token,(('doc id, tf),idf)
idf = key_converted_token.map(lambda x: (x[0], x[1][2])).\
    reduceByKey(lambda x, y: x+y).\
    map(lambda x: (x[0], np.log(total_document/x[1])))

rdd = tf.join(idf)
rdd = rdd.join(dictionary)

# convert (doc id, (token, tfidf))
tfidf = rdd.map(lambda x: (x[1][0][0][0], (x[1][1], x[1][0][0][1]*x[1][0][1])))
tfidf_vec = tfidf.groupByKey()
tfidf_matrix = tfidf_vec.map(lambda x: (
    x[0], build_matrix(x[1], tatal_vocab_count)))

# convert the result into matrix and print where number of row is number of document and number of colunm is vocabulary size
sub_question_result = tfidf_matrix.map(lambda x: (1, x[1])).groupByKey().map(
    lambda x: ('matrix', final_matrix(x[1])))
print(sub_question_result.take(1)[0][1])


def pair_generation(value):
    return type(value)


def similarity_compute(term):
    target = dictionary.filter(lambda x: x[0] == term)

    # get the target term index
    index = target.take(1)[0][1]
    word = target.take(1)[0][0]

    # compute the inner production between vector of target word and other word vectors
    production = tfidf_matrix.map(lambda x: (
        1, x[1][index]*x[1])).reduceByKey(lambda x, y: x+y)

    # sqrt
    sqrt = tfidf_matrix.map(lambda x: (1, x[1]**2)).reduceByKey(lambda x, y: x+y).map(
        lambda x: (x[0], np.sqrt(x[1]))).map(lambda x: (x[0], x[1][index]*x[1]))

    # join to get the product and sqrted vector (1, (product, sqrt))
    squre = production.join(sqrt).map(lambda x: (1, x[1][0] / x[1][1]))

    # argsort with descent method to get the index
    pairs = squre.flatMap(lambda x: [(y, word)
                                     for y in list(x[1].argsort()[::-1])])

    # process will be (word index, word) join (word index, target word) => (word index, (target word,word index))
    # => (target, word index)
    return pairs.join(dictionary.map(lambda x: (x[1], x[0]))).map(lambda x: (x[1][0], x[1][1])).filter(lambda x: x[1] != word)


res = similarity_compute('expression')
print(res.collect())
