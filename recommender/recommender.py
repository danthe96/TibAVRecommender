from pymantic import sparql
from tabulate import tabulate

server = sparql.SPARQLServer('http://danolithe.com:9999/bigdata/sparql')

def printResult(data):
	results = list()
	for res in data['results']['bindings']:
		line = list()
		for key in data['head']['vars']:
			line += [res.get(key, dict()).get('value', None)]
		results += [line]

	print tabulate(results, headers=data['head']['vars'])
	print str(len(results)) + ' result(s)'

def processResult(data):
	results = list()
	for res in data['results']['bindings']:
		line = dict()
		for key in data['head']['vars']:
			line[key] = res.get(key, dict()).get('value', None)
		results += [line]

	return results