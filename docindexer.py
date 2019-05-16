import os
from whoosh.index import create_in
from whoosh.lang.porter import stem
#from whoosh.fields import Schema, TEXT, ID
from whoosh import fields
from whoosh.analysis import CharsetFilter, StemmingAnalyzer, filters, tokenizers
from whoosh.support.charset import accent_map
from whoosh import qparser
from whoosh import scoring
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from whoosh.qparser import QueryParser

import nltk

#from whoosh.analysis.tokenizers import *
#from whoosh.analysis.filters import *

import sys
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

    myanalyzer = tokenizers.RegexTokenizer() | filters.LowercaseFilter() | filters.StopFilter() | MyLemmatizationFilter()
    #stem_ana = StemmingAnalyzer() | CharsetFilter(accent_map)
    schema = fields.Schema(document=fields.TEXT(analyzer=myanalyzer, stored=True),
                            sentenceid=fields.ID(stored=True),
                            rawtext=fields.TEXT(stored=True),
                            text=fields.TEXT(analyzer=myanalyzer, stored=True))

    #add tags/names/etc
    if not os.path.exists(indexpath):
        os.mkdir(indexpath)

    mprint("Creating a index writer to add document as per schema")
    # Creating a index writer to add document as per schema
    ix = create_in(indexpath, schema)
    #writer = ix.writer()


    writer = ix.writer(limitmb=256, procs=4) # , multisegment=True) #use 4 processors with 256 memory each
    '''
    # Get the analyzer object from a text field
    stem_ana = writer.schema["textdata"].format.analyzer
    # Set the cachesize to -1 to indicate unbounded caching
    stem_ana.cachesize = -1
    # Reset the analyzer to pick up the changed attribute
    stem_ana.clear()
    '''

    basepath = os.path.join(os.path.dirname(os.path.realpath('__file__')), root)
    mprint("basepath "+ basepath)
    filepaths = [os.path.join(root, i) for i in os.listdir(basepath)]
    count = 0
    for path in filepaths:
        basepath=os.path.join(os.path.dirname(os.path.realpath('__file__')), path)
        #print(basepath)
        with open(basepath, 'r') as wikifile:
            for line in wikifile:
                text = line.split(maxsplit=2)
                writer.add_document(document=text[0], sentenceid=text[1],
                                    rawtext=text[2], text=text[2])

        count+=1
        mprint("File "+str(count)+', Name '+path+" processed.")
        '''
        if count >=1:
            break
        '''
    mprint("Total "+str(count)+" file(s) processed, committing index")
    writer.commit()
    mprint("Commit complete!")

os.path.join(os.path.dirname(os.path.realpath('__file__')), "train.json")

#root = "corpus"
#root = "project/wiki-pages-text"
root = "wiki-pages-text"
indexpath = "wiki-index"
start = time.time()
#root = "wiki-pages-text"
createDocumentIndex(root, indexpath)
end = time.time()
print("Duration: ",end-start)

''' '''

def searchIndex(indexpath, query_str, topN=10):
    ix = open_dir(indexpath)

    with ix.searcher(weighting=scoring.Frequency) as searcher:
        # have or keyword parser, give preference to docs with more keywards combination found rather than one keyword repeated more
        og = qparser.OrGroup.factory(0.9)
        #parser = QueryParser("text", ix.schema, group=qparser.OrGroup)
        #search in both doc title and text
        parser = MultifieldParser(["document", "text"], schema=ix.schema, group=og)
        # Add plugin to use Damerau-Levenshtein edit distance, but require changing the query text to have it
        parser.add_plugin(qparser.FuzzyTermPlugin())
        query = parser.parse(query_str)
        results = searcher.search(query)#,limit=topN)
        print("No of results found:",len(results))
        if len(results) < topN:
            topN = len(results)
        for i in range(topN):
            print(str(results[i].score),results[i]['document'],
            results[i]['sentenceid'],
            results[i]['text'])
