import os
from typing import List

from whoosh.index import create_in
from whoosh.lang.porter import stem
from whoosh import fields
from whoosh.analysis import CharsetFilter, StemmingAnalyzer, filters, tokenizers
from whoosh.index import open_dir
from whoosh.util.text import rcompile

import nltk
import time

lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()
class MyLemmatizationFilter(filters.Filter):
    def __call__(self, tokens):
        for t in tokens:
            '''
            if t.mode == 'query':
                ...
            else if t.mode == 'index':
                ...
            '''
            lemma = lemmatizer.lemmatize(t.text, 'v')
            if lemma == t:
                lemma = lemmatizer.lemmatize(t.text, 'n')
            t.text=lemma
            yield t

def mprint(text):
    print(time.strftime("%a, %d %b %Y %H:%M:%S +0000"),text)

def createDocumentIndex(root, indexpath = "wiki-index"):
    '''
    Schema definition: title(name of file), path(as ID), content(indexed
    but not stored),textdata (stored text content)
            s = Schema(content = TEXT,
                       title = TEXT(stored = True),
                       tags = KEYWORD(stored = True),
                       date = DATETIME)
           Typical fields might be “title”, “content”, “url”, “keywords”, “status”, “date”, etc
           Storing the field makes it available in search results. For example, you typically want to store the “title” field so your search results can display it.
    '''
    '''
    schema = Schema(document=TEXT(stored=True),
                    sentinceid=ID(stored=True),
                    content=TEXT,
                    textdata=TEXT(stored=True))
    '''

    STOP_WORDS = frozenset(('a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'can',
                            'for', 'from', 'have', 'if', 'in', 'is', 'it', 'may',
                            'not', 'of', 'on', 'or', 'tbd', 'that', 'the', 'this',
                            'to', 'us', 'we', 'when', 'will', 'with', 'yet',
                            'you', 'your', ',', '.', ';', '-LRB-', '-LSB-', '-RSB-', '-RRB-', '-COLON-'))

    # Split document title to tokens based on '_'
    token_pattern = rcompile(r"[^_]+")
    titleanalyzer = tokenizers.RegexTokenizer(expression=token_pattern) | filters.LowercaseFilter() | \
        filters.StopFilter(stoplist=STOP_WORDS) | MyLemmatizationFilter()

    contentanalyzer = tokenizers.RegexTokenizer() | filters.LowercaseFilter() | filters.StopFilter(
        stoplist=STOP_WORDS) | MyLemmatizationFilter()
    #myanalyzer = tokenizers.RegexTokenizer() | filters.LowercaseFilter() | filters.StopFilter() | MyLemmatizationFilter()

    #myanalyzer = StemmingAnalyzer() | filters.LowercaseFilter() | filters.StopFilter(stoplist=STOP_WORDS)
    #stem_ana = StemmingAnalyzer() | CharsetFilter(accent_map)
    schema = fields.Schema(doctitle=fields.TEXT(analyzer=titleanalyzer, stored=True),
                            sentenceid=fields.ID(stored=True),
                            #rawtext=fields.TEXT(stored=True),
                            text=fields.TEXT(analyzer=contentanalyzer, stored=True))

    basepath = os.path.join(os.path.dirname(os.path.realpath('__file__')), root)
    mprint("basepath "+ basepath)
    filepaths = sorted([os.path.join(root, i) for i in os.listdir(basepath)])

    # Do indexing and writer commit incrementally in batches to avoid huge temporary index size and overkill during final commit
    i = 0
    BATCH_SIZE = 5
    while i <= len(filepaths):
        paths = filepaths[i:i+BATCH_SIZE]
        i += BATCH_SIZE
        #add tags/names/etc
        if not os.path.exists(indexpath):
            os.mkdir(indexpath)
            ix = create_in(indexpath, schema)
        else:
            ix = open_dir(indexpath)
        mprint("processing fileset "+str(paths))
        createIncrementalDocument(ix, paths)


def createIncrementalDocument(ix, filepaths):
    mprint("Creating a index writer to add document as per schema")
    # Creating a index writer to add document as per schema
    #writer = ix.writer()


    writer = ix.writer(limitmb=256) #, procs=4) # , multisegment=True) #use 4 processors with 256 memory each
    '''
    # Get the analyzer object from a text field
    stem_ana = writer.schema["textdata"].format.analyzer
    # Set the cachesize to -1 to indicate unbounded caching
    stem_ana.cachesize = -1
    # Reset the analyzer to pick up the changed attribute
    stem_ana.clear()
    '''
    count = 0
    for path in filepaths:
        basepath = os.path.join(os.path.dirname(os.path.realpath('__file__')), path)
        #print(basepath)
        count += 1
        mprint("Processing File No " + str(count) + ', Name ' + path)

        with open(basepath, 'r') as wikifile:
            for line in wikifile:
                text: List[str] = line.split(maxsplit=2)
                if len(text) >= 3:
                    writer.add_document(doctitle=text[0], sentenceid=text[1],
                                        #rawtext=text[2],
                                        text=text[2])
                else:
                    print("Dropping invalid line with content: '", line, "' not indexed.")

        '''
        if count >=2:
            break
        '''
    mprint("Total "+str(count)+" file(s) processed, committing index")
    writer.commit()
    mprint("Commit complete!")

#os.path.join(os.path.dirname(os.path.realpath('__file__')), "train.json")

#root = "corpus"
#root = "project/wiki-pages-text"
root = "wiki-pages-text"
indexpath = "wiki-index"
start = time.time()
#root = "wiki-pages-text"
createDocumentIndex(root, indexpath)
end = time.time()
print("Duration: ",end-start)


'''
basepath = os.path.join(os.path.dirname(os.path.realpath('__file__')), root)
mprint("basepath " + basepath)
filepaths = [os.path.join(root, i) for i in os.listdir(basepath)]
count = 0
for path in filepaths:
    basepath = os.path.join(os.path.dirname(os.path.realpath('__file__')), path)
    # print(basepath)
    count += 1
    mprint("Processing File No " + str(count) + ', Name ' + path)
    # ''
    if count >= 10:
        break
    # ''
#original lemaa    
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 22:10:43 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 22:10:43 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 22:10:43 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 22:11:52 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 22:12:59 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 22:14:41 +0000 Commit complete!
Duration:  238.26619815826416

#add remove list
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 22:28:50 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 22:28:50 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 22:28:50 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 22:30:34 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 22:32:26 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 22:34:42 +0000 Commit complete!
Duration:  352.54701495170593

#use StemmingAnalyzer() 
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 22:37:42 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 22:37:42 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 22:37:42 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 22:39:01 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 22:40:11 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 22:42:11 +0000 Commit complete!
Duration:  269.4476487636566


#use StemmingAnalyzer() with custom stop list
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 22:44:34 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 22:44:34 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 22:44:34 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 22:46:33 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 22:47:39 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 22:49:35 +0000 Commit complete!
Duration:  301.4200372695923


#use myLematizer() with custom stop list
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 23:11:56 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 23:11:56 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 23:11:56 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 23:13:27 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 23:14:29 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 23:16:12 +0000 Commit complete!
Duration:  256.0389471054077


#use limitmb=256
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 23:06:37 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 23:06:37 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 23:06:37 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 23:07:57 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 23:09:05 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 23:10:44 +0000 Commit complete!
Duration:  246.86932587623596

#use limitmb=256 & proc=4
/Users/ghawady/anaconda3/bin/python /Users/ghawady/Projects/Python/WSTA/project/docindexer.py
Fri, 17 May 2019 23:18:27 +0000 Creating a index writer to add document as per schema
Fri, 17 May 2019 23:18:27 +0000 basepath /Users/ghawady/Projects/Python/WSTA/project/wiki-pages-text
Fri, 17 May 2019 23:18:27 +0000 Processing File No 1, Name wiki-pages-text/wiki-001.txt
Fri, 17 May 2019 23:19:25 +0000 Processing File No 2, Name wiki-pages-text/wiki-002.txt
Fri, 17 May 2019 23:19:53 +0000 Total 2 file(s) processed, committing index
Fri, 17 May 2019 23:22:35 +0000 Commit complete!
Duration:  247.46253371238708


'''