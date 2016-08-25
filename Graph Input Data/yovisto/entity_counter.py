import collections

with open('yovistoextract_dbpedia_filtered.txt', 'r') as f_in:
	with open('yovistoextract_dbpedia_filtered_count.txt', 'w') as f_out:
		yovisto_dbp_list = list(f_in)
		dbp_list = []
		yovisto_list = []

		for line in yovisto_dbp_list:
			dbp_list.append(line.split()[1])
			yovisto_list.append(line.split()[0])

		dbp_count = collections.Counter(dbp_list)
		yovisto_count = collections.Counter(yovisto_list)
		for line in yovisto_dbp_list:
			f_out.write(line[:-2] + " " + str(yovisto_count[line.split(' ')[0]]) + " " + str(dbp_count[line.split(' ')[1]]) + "\r\n")


		f_in.close()
		f_out.close()

