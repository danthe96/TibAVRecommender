import sys
from recommender import recommender
import collections
#from tabulate import tabulate


def recommend():
	with open ("results.txt", "w") as outfile:
		for i in range(10):
			with open ("queries/subject_query.txt", "r") as f:
				video_query = f.read()
				original_video = recommender.server.query(video_query + str(i * 10000))
				outfile.write(recommender.printResult(original_video).encode('utf-8'))

	

def main():
	recommend()

if  __name__ =='__main__':main()