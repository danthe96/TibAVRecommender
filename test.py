from pymantic import sparql
from tabulate import tabulate

server = sparql.SPARQLServer('http://danolithe.com:9999/bigdata/sparql')

# Executing query

def printResult(result):
	results = list()
	for res in result['results']['bindings']:
		line = list()
		for key in res:
			line += [res[key]['value']]
		results += [line]

	print tabulate(results, headers=result['head']['vars'])

def main():
	result = server.query("""
		PREFIX dcterm: <http://purl.org/dc/terms/>
		PREFIX gnd: <http://d-nb.info/gnd/>
		PREFIX schema: <http://schema.org/>
		PREFIX tib: <http://av.tib.eu/resource/>
		PREFIX itsrdf: <http://www.w3.org/2005/11/its/rdf#>
		PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
		PREFIX oa: <http://w3.org/ns/oa#>
		PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

		SELECT ?coursename ?videoname ?videoid ?video2name ?video2id
		    WHERE {
		          ?video dcterm:identifier ?videoid .
		          ?video2 dcterm:identifier ?video2id.      
		          FILTER(?video2id > ?videoid) .
		      
		        ?video schema:name ?videoname .
		          ?video dcterm:isPartOf ?course .
		      
		          ?video2 dcterm:isPartOf ?course .
		          ?video2 schema:name ?video2name .
		          
		          ?course schema:name ?coursename .
		      
		          FILTER(?videoid = '11231').
		    }
		""")
	printResult(result)

if  __name__ =='__main__':main()