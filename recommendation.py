import sys
from recommender import recommender
import collections
from tabulate import tabulate

def collection_recommendation(video_id):
	server = recommender.server
	with open ("queries/collection_query.txt", "r") as f:
		query = f.read()

	result = server.query(query % video_id)
	return result


def author_recommendation(video_id):
	server = recommender.server
	with open ("queries/author_query.txt", "r") as f:
		query = f.read()

	result = server.query(query % video_id)
	return result


def recommend(video_id):
	with open ("queries/video_query.txt", "r") as f:
		video_query = f.read()
		original_video = recommender.server.query(video_query % video_id)
		print "Original video:"
		recommender.printResult(original_video)

	collection_rec_data = collection_recommendation(video_id)
	#recommender.printResult(collection_rec_data)
	collection_rec = recommender.processResult(collection_rec_data)

	author_rec_data = author_recommendation(video_id)
	#recommender.printResult(author_rec_data)
	author_rec = recommender.processResult(author_rec_data)

	collectionMultiplier = 1
	authorMultiplier = 0.3
	publisherMultiplier = 0.3

	recommendationCounter = collections.Counter()
	for record in collection_rec:
		recommendationCounter[record['video2id']] += collectionMultiplier
	for record in author_rec:
		recommendationCounter[record['video2id']] += authorMultiplier

	print ""
	print "========================================================="
	print "RECOMMENDATIONS"
	print "========================================================="

	for (i, (rec_id, score)) in enumerate(recommendationCounter.most_common(5)):
		print str(i+1) + '. (Score: ' + str(score) + ')'
		rec_info = recommender.server.query(video_query % rec_id)
		recommender.printResult(rec_info)
		print ""


def main():
	recommend(sys.argv[1])

if  __name__ =='__main__':main()