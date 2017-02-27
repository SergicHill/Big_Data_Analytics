import operator
import nltk
from nltk.corpus import inaugural
from nltk.corpus import wordnet as wn


#fdist1 = nltk.FreqDist(inaugural)
#fdist1 = nltk.FreqDist(text4)

#fdist1 = nltk.FreqDist(w.lower() for w in nltk.corpus.gutenberg.words(inaugural))
 
d={}
for fileid in inaugural.fileids():
    fdist = nltk.FreqDist(w.lower() for w in nltk.corpus.inaugural.words(fileid))
    d={k: d.get(k, 0) + fdist.get(k, 0) for k in set(d) | set(fdist)}

# filter keys of length in excess of 7
d = {k: v for k, v in d.items() if len(k) > 7}

# sort dictionary by values
v0=[k for k in sorted(d.items(), key=operator.itemgetter(1), reverse=True)]


for i in range(1,20):
    print (i, v0[i])

car = wn.synsets('motorcar')
wn.synset(car)[0].lemma_names()
wn.synset('car.n.01').lemma_names()
wn.synsets('dog')[0].definition()
>>> for synset in wn.synsets('car'):
...     print (synset.lemma_names())




