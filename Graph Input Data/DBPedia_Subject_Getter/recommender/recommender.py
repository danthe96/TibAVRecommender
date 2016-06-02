from pymantic import sparql
from tabulate import tabulate

server = sparql.SPARQLServer('http://dbpedia.org/sparql')

def printResult(data):
	results = list()
	for res in data['results']['bindings']:
		line = list()
		for key in data['head']['vars']:
			line += [res.get(key, dict()).get('value', None)]
		results += [line]

	string = tabulate(results, headers=data['head']['vars'])
	#print str(len(results)) + ' result(s)'
	return string

def processResult(data):
	results = list()
	for res in data['results']['bindings']:
		line = dict()
		for key in data['head']['vars']:
			line[key] = res.get(key, dict()).get('value', None)
		results += [line]

	return results
	
def countLines(data):
	count =0;
	for res in data['results']['bindings']:
		line = list()
		count = count +1
	return count