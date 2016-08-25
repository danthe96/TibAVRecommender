from sets import Set

f_tib = open('GND_DBPEDIA_filtered_sorted_count.txt', 'r')
f_tib_yago = open('yago_types_filtered_filtered_sorted_count.txt', 'r')
f_yov_yago = open('yovisto_dbpedia_yago.txt', 'r')
f = open('yovistoextract_dbpedia_filtered.txt', 'r')
f_out_dbp = open('yovistoextract_dbpedia_filtered_small.txt', 'w')
f_out_yago = open('yovisto_dbpedia_yago_filtered.txt', 'w')

tibdbpSet = Set()
for line in f_tib:
	tibdbpSet.add(line.split()[1])
f_tib.close()

tibyagoSet = Set()
for line in f_tib_yago:
	tibyagoSet.add(line.split()[1])
f_tib_yago.close()

dbp_yago = {}
for line in f_yov_yago:
	dbp_yago[line.split()[0]] = line.split()[1]

lineSet = Set()
for line in f:
	try:
		if line.split()[1] in tibdbpSet or dbp_yago[line.split()[1]] in tibyagoSet:
			lineSet.add(line)
			f_out_yago.write(line.split()[1] + " " + dbp_yago[line.split()[1]] + "\n")
	except Exception, e:
		if line.split()[1] in tibdbpSet:
			lineSet.add(line)

for line in lineSet:
	f_out_dbp.write(line)
	