import collections

with open('yago_types_collected_full_filtered_count.txt', 'r') as f_in:
	with open('yago_types_collected_full_filtered_count_2.txt', 'w') as f_out:
		entity_type_list = list(f_in)
		type_list = []
		entity_list = []
		entity_type_set = set(entity_type_list)
		for line in entity_type_set:
			type_list.append(line.split(' ')[1])
			entity_list.append(line.split()[0])

		type_count = collections.Counter(type_list)
		entity_count = collections.Counter(entity_list)
		for line in entity_type_set:
			if line.split(' ')[1].endswith("\n"):
				f_out.write(line.split(' ')[0] + " " + line.split(' ')[1][:-2] + " " + str(entity_count[line.split(' ')[0]]) + " " + str(type_count[line.split(' ')[1]]) + "\r\n")
			else:
				f_out.write(line.split(' ')[0] + " " + line.split(' ')[1] + " " + str(entity_count[line.split(' ')[0]]) + " " + str(type_count[line.split(' ')[1]]) + "\r\n")

		f_in.close()
		f_out.close()

