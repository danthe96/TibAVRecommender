import feedparser
from pyRdfa import pyRdfa

python_wiki_rss_url = "http://blog.yovisto.com/feed/"
feed = feedparser.parse( python_wiki_rss_url )
entries =  feed[ "items" ] 
sources = []

f = open('yovistoextract.ttl', 'w')

for i in range(2):
	sources.append(entries[i]["link"])

for source in sources:
	print source
f.write(pyRdfa().rdf_from_sources(sources))

f.close()