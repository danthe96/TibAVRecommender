import collections

f_in = open('yago_supertypes.ttl', 'r')
f_out = open('yago_supertypes_filtered_sorted_count.txt', 'wb')
type_to_type_list = list(f_in)
left_type_list = []
right_type_list = []


for line in type_to_type_list:
	left_type_list.append(line.split(' ')[0])
	right_type_list.append(line.split(' ')[1])

right_type_list_count = collections.Counter(right_type_list)
left_type_list_count = collections.Counter(left_type_list)
for line in type_to_type_list:
	f_out.write(line[:-2] + " " + str(left_type_list_count[line.split(' ')[0]]) + " " + str(right_type_list_count[line.split(' ')[1]]) + "\r\n")


f_in.close()
f_out.close()
