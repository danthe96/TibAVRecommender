import collections

f_in = open('tib_gnd_sorted_count.txt', 'r')
f_out = open('tib_gnd_sorted_count_with_gnd.txt', 'w')
entity_gnd_list = list(f_in)
gnd_list = []
entity_list = []

for line in entity_gnd_list:
	entity_list.append(line.split(' ')[0])
	gnd_list.append(line.split(' ')[1])

gnd_count = collections.Counter(gnd_list)
gnd_total_count = collections.Counter()
for line in entity_gnd_list:
	gnd_total_count[line.split(' ')[1]] += int(line.split(' ')[2])

entity_count = collections.Counter(entity_list)
for line in entity_gnd_list:
	f_out.write(line[:-2] + " " + str(gnd_total_count[line.split(' ')[1]]) + "\r\n")


f_in.close()
f_out.close()
