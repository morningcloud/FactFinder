import os

from whoosh import qparser
from whoosh import scoring
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from whoosh.analysis import filters

import nltk

lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()

class MyLemmatizationFilter(filters.Filter):
    def __call__(self, tokens):
        for t in tokens:
            #incase differernt logic needed for query vs index
            '''
            if t.mode == 'query':
                ...
            else if t.mode == 'index':
                ...
            '''
            #todo: Exclude encoded qoutes
            lemma = lemmatizer.lemmatize(t.text, 'v')
            if lemma == t:
                lemma = lemmatizer.lemmatize(t.text, 'n')
            t.text=lemma
            yield t

def searchIndex(indexpath, query_str, topN=10):
    ix = open_dir(indexpath)

    #For example, to use "BM25" for most fields, but ``Frequency`` for the "id" field and ``TF_IDF`` for the ``keys`` field::
    #MultiWeighting(BM25(), doctitle=Frequency(), text=TF_IDF())
    with ix.searcher(weighting=scoring.TF_IDF()) as searcher:
        # have or keyword parser, give preference to docs with more keywards combination found rather than one keyword repeated more
        orCondition = qparser.OrGroup.factory(0.9)
        #parser = QueryParser("text", ix.schema, group=qparser.OrGroup)
        #search in both doc title and text
        #Commented the or condition to get queryies based on and... need experimenting with the claims
        parser = MultifieldParser(["doctitle", "text"], schema=ix.schema)#, group=orCondition)
        # Add plugin to use Damerau-Levenshtein edit distance, but require changing the query text to have it
        parser.add_plugin(qparser.FuzzyTermPlugin())
        query = parser.parse(query_str)
        #print(searcher.document())
        #results = searcher.search(query)
        results = searcher.search(query, limit=topN)
        print("No of results found:", len(results))
        if len(results) < topN:
            topN = len(results)
        print(type(results[0]))
        for i in range(topN):
            print(str(results[i].score), results[i]['doctitle'],
                  results[i]['sentenceid'],
                  results[i]['text'])
        return results


indexpath = "wiki-index"
fullpath = os.path.join(os.path.dirname(os.path.realpath('__file__')), indexpath)
query_str="alternative theory"
topN = 2
searchIndex(fullpath, query_str, topN)
