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

f_in = open('DBPedia_types_filtered.txt', 'r')
f_out = open('DBPedia_types_filtered_count.txt', 'wb')
entity_type_list = list(f_in)
type_list = []

for line in entity_type_list:
	type_list.append(line.split(' ')[1])

type_count = collections.Counter(type_list)
for line in entity_type_list:
	f_out.write(line[:-2] + " " + str(type_count[line.split(' ')[1]]) + "\r\n")
