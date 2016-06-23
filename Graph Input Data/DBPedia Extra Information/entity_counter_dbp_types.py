import collections

f_in = open('DBPedia_types_filtered.ttl', 'r')
f_out = open('DBPedia_types_filtered_count.txt', 'wb')
entity_type_list = list(f_in)
type_list = []
entity_list = []

for line in entity_type_list:
	entity_list.append(line.split(' ')[0])
	type_list.append(line.split(' ')[1])

type_count = collections.Counter(type_list)
entity_count = collections.Counter(entity_list)
for line in entity_type_list:
	f_out.write(line[:-2] + " " + str(entity_count[line.split(' ')[0]]) + " " + str(type_count[line.split(' ')[1]]) + "\r\n")


f_in.close()
f_out.close()
