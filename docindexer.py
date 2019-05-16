import os
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID
import sys
import time

def createSearchableData(root):
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
    schema = Schema(title=TEXT(stored=True),
                    path=ID(stored=True),
                    content=TEXT,
                    textdata=TEXT(stored=True))
    #add tags/names/etc
    indexpath = "indexdir"
    if not os.path.exists(indexpath):
        os.mkdir(indexpath)

    print("Creating a index writer to add document as per schema")
    # Creating a index writer to add document as per schema
    ix = create_in(indexpath, schema)
    writer = ix.writer()

    basepath = os.path.join(os.path.dirname(os.path.realpath('__file__')), root)
    print("basepath",basepath)
    filepaths = [os.path.join(root, i) for i in os.listdir(basepath)]
    count = 0
    for path in filepaths:
        basepath=os.path.join(os.path.dirname(os.path.realpath('__file__')), path)
        #print(basepath)
        fp = open(basepath, 'r')
        text = fp.read()
        writer.add_document(title=path.split("/")[1], path=path,
                            content=text, textdata=text)
        fp.close()
        '''
        print(count,'-',path.split("/")[1],path)
        count+=1
        if count >=10:
            break
        '''
    writer.commit()

os.path.join(os.path.dirname(os.path.realpath('__file__')), "train.json")

root = "corpus"
start = time.time()
#root = "wiki-pages-text"
createSearchableData(root)
end = time.time()
print("Duration: ",end-start)

'''
from whoosh.qparser import QueryParser
from whoosh import scoring
from whoosh.index import open_dir
 
ix = open_dir("indexdir")
 
# query_str is query string
query_str = "doc"
# Top 'n' documents as result
topN = int(5)
 
with ix.searcher(weighting=scoring.Frequency) as searcher:
    query = QueryParser("textdata", ix.schema).parse(query_str)
    results = searcher.search(query,limit=topN)
    for i in range(topN):
        print(results[i]['title'], str(results[i].score),
        results[i]['textdata'])
'''