import operator
import nltk
from nltk.corpus import inaugural
from nltk.corpus import wordnet as wn

 
#identify frequency of words used in all texts related to inaugural
d={}
for fileid in inaugural.fileids():
    fdist = nltk.FreqDist(w.lower() for w in nltk.corpus.inaugural.words(fileid))
    d={k: d.get(k, 0) + fdist.get(k, 0) for k in set(d) | set(fdist)}

# filter keys of length in excess of 7
d = {k: v for k, v in d.items() if len(k) > 7}

# sort dictionary by values
v0=[k for k in sorted(d.items(), key=operator.itemgetter(1), reverse=True)]

#Take the first 10 items and place them into dictionary ( word: [synonyms]
dsyn={}
# dictionary for [word: length of synonyms]
dmaxNumSyn={}
for i in range(1,10):
#  get  Synset of a particular word 'i' held in v0[i][0]
    tmpv = wn.synsets(v0[i][0])
    len_syn = len(tmpv)
#	 Get a set, so far empty, for synonymous
    set_synonims = set()
#   Fill the set with synonymous of lemma_names	
    for k in range(0, len_syn-1):
        set_synonims.update(tmpv[k].lemma_names())
#Fill the dictionary dsyn		
    dsyn[v0[i][0]]=set_synonims
    dmaxNumSyn[v0[i][0]]=len(set_synonims)


print('LIST ALL SYNONYMS FOR THOSE 10 WORDS')
for i in range(1,10): 
    print(v0[i][0],' : ', dsyn[v0[i][0]] ,'   \n')
	
print('LIST ALL WORDS AND THE NUMBER OF THEIR  SYNONYMS')
print(dmaxNumSyn)

#sort 
dmaxsyn=[k for k in sorted(dmaxNumSyn.items(), key=operator.itemgetter(1), reverse=True)]


print('The word with maximum number of synonyms is  \n')
print(dmaxsyn[0])
print('------------------------------------------------')

dhyp={}
# dictionary for [word: length of synonyms]
dmaxNumHyp={}
for i in range(1,10):
#  get  Synset of a particular word 'i' held in v0[i][0]
    tmpv = wn.synsets(v0[i][0])
    len_syn = len(tmpv)
#	 Get a set, so far empty, for hyponyms
    set_hyponyms = set()
#   Fill the set with hyponyms of lemma_names	
    for k in range(0, len_syn-1):
        tmp=[lemma.name() for synset in tmpv[k].hyponyms() for lemma in synset.lemmas()]
        set_hyponyms.update(tmp)
#Fill the dictionary 		
    dhyp[v0[i][0]] = set_hyponyms
    dmaxNumHyp[v0[i][0]]=len(set_hyponyms)



print('LIST ALL HYPONYMS FOR THOSE 10 WORDS')
for i in range(1,10): 
    print(v0[i][0],' : ', dhyp[v0[i][0]] ,'   \n')

#sort 
dmaxhyp=[k for k in sorted(dmaxNumHyp.items(), key=operator.itemgetter(1), reverse=True)]

print('The word with maximum number of hyponyms is  \n')
print(dmaxhyp[0])




