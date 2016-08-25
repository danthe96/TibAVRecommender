from sets import Set

f_tib = open('yago_supertypes.ttl', 'r')
f_yovisto = open('yovisto_yago_super_3.txt', 'r')
f_merge = open('merge_super_types.txt', 'w')

superTypes = Set()
for line in f_tib:
	superTypes.add(line[:-1].replace("<", "").replace(">", ""))

for line in f_yovisto:
	superTypes.add(line[:-1])

for line in superTypes:
	f_merge.write(line + "\n")