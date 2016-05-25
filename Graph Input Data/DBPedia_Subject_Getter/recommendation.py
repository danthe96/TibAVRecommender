import sys
from recommender import recommender
import collections
#from tabulate import tabulate


def recommend():
	i = 0
	with open ("results.txt", "w") as outfile:
		with open ("queries/subject_query.txt", "r") as f:
			video_query = f.read()
			original_video = recommender.server.query(video_query + (i * 10000))
			outfile.write(recommender.processResult(original_video))

	

def main():
	recommend()

if  __name__ =='__main__':main()