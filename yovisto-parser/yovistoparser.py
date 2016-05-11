import feedparser
from pyRdfa import pyRdfa

python_wiki_rss_url = "http://blog.yovisto.com/feed/"
feed = feedparser.parse( python_wiki_rss_url )
entries =  feed[ "items" ] 
sources = []

f = open('yovistoextract.ttl', 'w')

for entry in entries:
	sources.append(entry["link"])

for source in sources:
	f.write(pyRdfa().rdf_from_source(source))

f.close()