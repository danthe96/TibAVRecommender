import collections

with open('yago_types_filtered.ttl', 'r',encoding='utf8') as f_in:
	with open('yago_types_filtered_sorted_count.ttl', 'w',encoding='utf8') as f_out:
		entity_type_list = list(f_in)
		type_list = []

		for line in entity_type_list:
			type_list.append(line.split(' ')[1])

		type_count = collections.Counter(type_list)
		for line in entity_type_list:
			f_out.write(line[:-1] + " " + str(type_count[line.split(' ')[1]]) + "\r\n")


		f_in.close()
		f_out.close()

