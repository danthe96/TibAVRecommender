import collections

f_in = open('tib_gnd_sorted.txt', 'r')
f_out = open('tib_gnd_sorted_count.txt', 'wb')
video_entities = list(f_in)
videos = []

for line in video_entities:
	videos.append(line.split(' ')[0])

video_entity_count = collections.Counter(video_entities).most_common()
video_count = collections.Counter(videos)
for (entity, count) in video_entity_count:
	f_out.write(entity[:-1] + " " + str(count) + " " + str(video_count[entity.split(' ')[0]]) + "\r\n")

f_in.close()
f_out.close()

