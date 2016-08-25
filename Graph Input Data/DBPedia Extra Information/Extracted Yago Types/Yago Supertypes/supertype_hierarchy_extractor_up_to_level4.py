f_in = open('yago_supertypes_filtered_sorted_count_with_hierarchy.txt', 'r')
f_out = open('yago_supertypes_with_hierarchy_up_to_level4.txt', 'wb')

for line in f_in:
	if int(line.split(' ')[5]) <= 4:
		f_out.write(line)
f_in.close()
f_out.close()
