import sys
from recommender import recommender
import collections
from tabulate import tabulate


def recommend():
	limit = 10000
	count = limit
	i=0
	total =0
	with open ("results_english_dbpedia_subjects.txt", "w") as outfile:
		while count >=limit:
			with open ("queries/subject_query.txt", "r") as f:
				video_query = f.read()
				original_video = recommender.server.query(video_query + str(i * limit))
				results = recommender.printResult(original_video).encode('utf-8')
				outfile.write(results)
				count = recommender.countLines(original_video)
				total = total + count
				print total
				i = i+1
	

def main():
	recommend()

if  __name__ =='__main__':main()