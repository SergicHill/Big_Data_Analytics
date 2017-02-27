import operator
import nltk
from nltk.book import gutenberg

modals=['can', 'could', 'may', 'might', 'must', 'will', 'would', 'should']

# create dictionary
d={}
# fill dictionary with relative frequencies
for fileid in gutenberg.fileids():
    d[fileid]={}
    fdist = nltk.FreqDist(w.lower() for w in nltk.corpus.gutenberg.words(fileid))
    total_words=len(nltk.corpus.gutenberg.words(fileid))
    for m in modals: 
#        d[fileid][m] = fdist[m]    
        d[fileid][m] = fdist[m]/total_words

# print dictionary
print('\n\n ------ PRINT DICTIONARY OF RELATIVE FREQUENCIES -------')
for fileid in gutenberg.fileids():
    print(fileid + ' ')
    for m in modals:     
        print(m,' : ', d[fileid][m] ,' ')
    print('\n')

# find the most and least used modals
d_most={}
d_name_most={}	
d_least={}
d_name_least={}
for m in modals:
    d_most[m]=-1
    d_least[m]=1000000
    d_name_least[m]={}
    d_name_most[m]={}
    for fileid in  gutenberg.fileids():
        if d[fileid][m] > d_most[m]:
            d_most[m] = d[fileid][m]
            d_name_most[m] = fileid		
        if d[fileid][m] < d_least[m]:
            d_least[m] = d[fileid][m]
            d_name_least[m] = fileid
			

#for m in modals:
#    print ('m=',m, ' d_most[m]=',d_most[m], '  d_name_most[m]=',d_name_most[m],'\n')	
#print('\n')
#for m in modals:
#    print ('m=',m, ' d_least[m]=',d_least[m], ' d_name_least[m]=',d_name_least[m],'\n')

# compute spans
d_span={}
for m in modals:
   d_span[m]= d_most[m]-d_least[m]
   
   
max_span_m0 = max(d_span.items(), key=operator.itemgetter(1))[0]
max_span_m1 = max(d_span.items(), key=operator.itemgetter(1))[1]

#sort the spans
v0=[k for k in sorted(d_span.items(), key=operator.itemgetter(1), reverse=True)]

print('The most used modals with the relative frequencies of their usage', '\n')
print('v0=',v0,'\n')

#select a text which uses it the most 
d_name_most=''
def textname_mostusedmodal(modal_ans1):
    imax=-1	
    for fileid in gutenberg.fileids():
        if d[fileid][modal_ans1] > imax:
            imax = d[fileid][modal_ans1]
            d_name_most = fileid
    return 	d_name_most, imax

#select a text which uses it the least	
def textname_leastusedmodal(modal_ans1):
    imin = 10000000	
    for fileid in gutenberg.fileids():
        if d[fileid][modal_ans1] < imin:
            imin = d[fileid][modal_ans1]
            d_name_most = fileid
    return 	d_name_most, imin

modal_ans1=v0[0][0]
d_name_most1, imax1 = textname_mostusedmodal(modal_ans1)

modal_ans2=v0[1][0]
d_name_most2, imax2 = textname_mostusedmodal(modal_ans2)

leastUsed1, imin1 = textname_leastusedmodal(modal_ans1)
leastUsed2, imin2 = textname_leastusedmodal(modal_ans2)

print('-----------------------------------------------------')
print('\n The most used modal :', modal_ans1, '\t in the file =',d_name_most1,'\t with relative frequency =',imax1, '\n')
print(' The second most used modal :', modal_ans2, '\t in the file=',d_name_most2,'\t with relative frequency =',imax2, '\n')
print('\n The least used modal=', modal_ans1, '\t in the file =',leastUsed1,'\t with relative frequency =',imin1, '\n')
print('\n The least used modal=', modal_ans2, '\t in the file =',leastUsed2,'\t with relative frequency=',imin2, '\n')
print('-----------------------------------------------------')

print('-----------------------------------------------------')
print('Concordances of the modal ',modal_ans1, ' that is the most used in text: ',d_name_most1,'\n' )
print('-----------------------------------------------------')
print( nltk.Text(nltk.corpus.gutenberg.words(d_name_most1)).concordance(modal_ans1)  )

print('-----------------------------------------------------')
print('Concordances of the modal ',modal_ans1, ' that is the least used in the text: ',leastUsed1,'\n' )
print('-----------------------------------------------------')
print( nltk.Text(nltk.corpus.gutenberg.words(leastUsed1)).concordance(modal_ans1)  )

	


	


