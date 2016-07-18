from pyRdfa import pyRdfa
from pyRdfa.options import Options
import xml.etree.ElementTree
from sets import Set


# options = Options(vocab_expansion=True)
# sources = []

# e = xml.etree.ElementTree.parse('data/post-sitemap.xml').getroot()

# print(e)
# i = 0
# for aurl in e:
# 	#if i <= 100:
# 	sources.append(aurl[0].text)
# 	i += 1

# f = open('data/yovistoextract_dbpedia.txt', 'w')
# #f = open('data/yovisto_dbpedia_yago.txt', 'r')



# i = 1

# for source in sources:
# 	nt = pyRdfa(options=options).rdf_from_source(source, outputFormat='nt')
# 	f.write(nt)
# 	print("finished " + str(i) + " of " + str(len(sources)))
# 	i += 1
# f.close()
f = open('data/yovistoextract_dbpedia.txt', 'r')
dbpSet = Set()

f_out = open('data/yovistoextract_dbpedia_filtered.txt', 'w')
for line in f:
	if '<http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource' not in line or 'http://blog.yovisto.com/' not in line:
		continue
	newline = line[:-2] + '\n'
	dbpSet.add(line.split()[2])
	f_out.write(newline.replace("<", "").replace(">", "").replace(" http://www.w3.org/2004/02/skos/core#subject ", " "))
f_out.close()


f_dbp_yago = open('data/yagoDBpediaInstances.tsv', 'r')
dbp_yago = {}	
for line in f_dbp_yago:
	if line.split()[1] == 'owl:sameAs':
		dbp_yago[line.split()[2]] = line.split()[0]
f_dbp_yago.close()

print "finished yagoDBpediaInstances"

f_yago_yago_class = open('data/yagoTypes.tsv', 'r')
yago_yago_class = {}	
for line in f_yago_yago_class:
	if line.split()[2] == 'rdf:type':
		if line.split()[1] not in yago_yago_class:
			yago_yago_class[line.split()[1]] = Set([line.split()[3]])
		else:
			yago_yago_class[line.split()[1]].add(line.split()[3])
f_yago_yago_class.close()

print "finished yagoTypes"

f_yago_class_dbp_class = open('data/yagoDBpediaClasses.tsv', 'r')
yago_class_dbp_class = {}
for line in f_yago_class_dbp_class:
	if line.split()[1] == 'owl:equivalentClass':
		yago_class_dbp_class[line.split()[0]] = line.split()[2]
f_yago_class_dbp_class.close()

print "finished yagoDBpediaClasses"


f_yago_super = open('data/yagoTaxonomy.tsv', 'r')
yago_super = {}
for line in f_yago_super:
	if line.split()[2] == 'rdfs:subClassOf':
			yago_super[line.split()[1]] = line.split()[3]
f_yago_super.close()

print "finished yagoDBpediaClasses" + str(len(yago_super.keys()))

yago_types = Set()
f_out = open("data/yovisto_dbpedia_yago.txt", "w")
for dbpEntity in dbpSet:
	try:
		yagoclassset = yago_yago_class[dbp_yago[dbpEntity]]
		for yagoclass in yagoclassset:
			yago_types.add(yagoclass)
			f_out.write(dbpEntity.replace("<", "").replace(">", "") + " " + yago_class_dbp_class[yagoclass].replace("<", "").replace(">", "")+"\n")
	except Exception, e:
		continue
f_out.close()

f_out = open('data/yovisto_yago_super_3.txt', 'w')
yago_types0 = yago_types.copy()
yago_types1 = Set()
for yagoclass in yago_types0:
	try:
		yagosuper = yago_super[yagoclass]
		f_out.write(yago_class_dbp_class[yagoclass].replace("<", "").replace(">", "") + ' ' + yago_class_dbp_class[yagosuper].replace("<", "").replace(">", "")+"\n")
		if yagosuper not in yago_types:
			yago_types1.add(yagosuper)
			yago_types.add(yagosuper)
	except Exception, e:
		continue
print "finished first level"

yago_types2 = Set()
for yagoclass in yago_types1:
	try:
		yagosuper = yago_super[yagoclass]
		f_out.write(yago_class_dbp_class[yagoclass].replace("<", "").replace(">", "") + ' ' + yago_class_dbp_class[yagosuper].replace("<", "").replace(">", "")+"\n")
		if yagosuper not in yago_types:
			yago_types2.add(yagosuper)
			yago_types.add(yagosuper)
	except Exception, e:
		continue
print "finished second level"

yago_types3 = Set()
for yagoclass in yago_types2:
	try:
		yagosuper = yago_super[yagoclass]
		f_out.write(yago_class_dbp_class[yagoclass].replace("<", "").replace(">", "") + ' ' + yago_class_dbp_class[yagosuper].replace("<", "").replace(">", "")+"\n")
		if yagosuper not in yago_types:
			yago_types3.add(yagosuper)
			yago_types.add(yagosuper)
	except Exception, e:
		continue
print "finished third level"


#nt = pyRdfa(options=options).rdf_from_source('http://blog.yovisto.com/roger-cotes-and-newtons-principia-mathematica/', outputFormat='nt')
#print(nt)
#f.write(nt)
f_out.close()