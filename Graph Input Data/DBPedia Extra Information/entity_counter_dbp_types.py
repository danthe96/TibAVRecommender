import collections

f_in = open('yago_types_filtered.ttl', 'r')
f_out = open('yago_types_filtered_count.txt', 'wb')
entity_type_list = list(f_in)
type_list = []


for line in entity_type_list:
	type_list.append(line.split(' ')[1])

type_count = collections.Counter(type_list)
entity_count = collections.Counter(entity_type_list)
for line in entity_type_list:
	f_out.write(line[:-2] + " " + str(type_count[line.split(' ')[1]]) + "\r\n")


f_in.close()
f_out.close()
