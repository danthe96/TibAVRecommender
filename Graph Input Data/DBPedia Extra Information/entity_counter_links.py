import collections

f_in = open('PAGE_LINKS.txt', 'r')
f_out = open('PAGe_LINKS_count.txt', 'wb')
entity_link_list = list(f_in)
entity_list = []


for line in entity_link_list:
	entity_list.append(line.split(' ')[0])

entity_count = collections.Counter(entity_list)
for line in entity_link_list:
	f_out.write(line[:-2] + " " + str(entity_count[line.split(' ')[0]]) + "\r\n")


f_in.close()
f_out.close()
