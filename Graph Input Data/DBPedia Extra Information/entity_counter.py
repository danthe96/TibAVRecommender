import collections

with open('merge_dbp_yago.txt', 'r') as f_in:
	with open('merge_dbp_yago_count.txt', 'w') as f_out:
		entity_type_list = list(f_in)
		type_list = []
		entity_list = []

		for line in entity_type_list:
			type_list.append(line.split(' ')[1])
			entity_list.append(line.split()[0])

		type_count = collections.Counter(type_list)
		entity_count = collections.Counter(entity_list)
		for line in entity_type_list:
			f_out.write(line[:-1] + " " + str(entity_count[line.split(' ')[0]]) + " " + str(type_count[line.split(' ')[1]]) + "\r\n")


		f_in.close()
		f_out.close()

