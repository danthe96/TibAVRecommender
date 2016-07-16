# from pyRdfa import pyRdfa
# from pyRdfa.options import Options
# import xml.etree.ElementTree
from sets import Set


# options = Options(vocab_expansion=True)
# sources = []

# e = xml.etree.ElementTree.parse('post-sitemap2.xml').getroot()

# print(e)

# for aurl in e:
#     sources.append(aurl[0].text)

#f = open('yovistoextract_dbpedia.txt', 'r')
f = open('yovisto_dbpedia_yago.txt', 'r')

f_types = open('DBPedia_types.txt', 'r')
f_out = open('yovisto_yago_super3.txt', 'w')

# i = 1

# for source in sources:
# 	nt = pyRdfa(options=options).rdf_from_source(source, outputFormat='nt')
# 	f.write(nt)
# 	print("finished " + str(i) + " of " + str(len(sources)))
# 	i += 1

# for line in f:
# 	if '<http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource' not in line:
# 		continue
# 	newline = line[:-2] + '\n'
#	f_out.write(newline.replace("<", "").replace(">", "").replace(" http://www.w3.org/2004/02/skos/core#subject ", " "))

# dbpSet = Set()
# for line in f:
# 	dbpSet.add(line.split()[1])

# for line in f_types:
# 	try:
# 		(key, value) = line.split()
# 	except Exception, e:
# 		print line.split()
# 		continue
# 	if key in dbpSet:
# 		f_out.write(key + ' ' + value + '\n')


# f_dbp_yago = open('yagoDBpediaInstances.tsv', 'r')
# dbp_yago = {}	
# for line in f_dbp_yago:
# 	if line.split()[1] == 'owl:sameAs':
# 		dbp_yago[line.split()[2]] = line.split()[0]
# f_dbp_yago.close()

# print "finished yagoDBpediaInstances"

# f_yago_yago_class = open('yagoTypes.tsv', 'r')
# yago_yago_class = {}	
# for line in f_yago_yago_class:
# 	if line.split()[2] == 'rdf:type':
# 		if line.split()[1] not in yago_yago_class:
# 			yago_yago_class[line.split()[1]] = Set([line.split()[3]])
# 		else:
# 			yago_yago_class[line.split()[1]].add(line.split()[3])
# f_yago_yago_class.close()

# print "finished yagoTypes"

f_yago_class_dpb_class = open('yagoDBpediaClasses.tsv', 'r')
yago_class_dpb_class = {}
for line in f_yago_class_dpb_class:
	if line.split()[1] == 'owl:equivalentClass':
		yago_class_dpb_class[line.split()[0]] = line.split()[2]
f_yago_class_dpb_class.close()

print "finished yagoDBpediaClasses"

dpb_class_yago_class = {}
for yago in yago_class_dpb_class.keys():
	dpb_class_yago_class[yago_class_dpb_class[yago]] = yago

f_yago_super = open('yagoTaxonomy.tsv', 'r')
yago_super = {}
for line in f_yago_super:
	if line.split()[2] == 'rdfs:subClassOf':
			yago_super[line.split()[1]] = line.split()[3]
f_yago_super.close()

print "finished yagoDBpediaClasses" + str(len(yago_super.keys()))

dbp_yago_types = Set()
for line in f:
	dbpyagoclass = "<"+line.split()[1]+">"
	dbp_yago_types.add(dbpyagoclass)
print "finished dbp_yago_types set"

yago_types = Set()
for dbpyago in dbp_yago_types:
	yago_types.add(dpb_class_yago_class[dbpyago])

yago_types0 = yago_types.copy()
yago_types1 = Set()
for yagoclass in yago_types0:
	try:
		yagosuper = yago_super[yagoclass]
		f_out.write(yago_class_dpb_class[yagoclass].replace("<", "").replace(">", "") + ' ' + yago_class_dpb_class[yagosuper].replace("<", "").replace(">", "")+"\n")
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
		f_out.write(yago_class_dpb_class[yagoclass].replace("<", "").replace(">", "") + ' ' + yago_class_dpb_class[yagosuper].replace("<", "").replace(">", "")+"\n")
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
		f_out.write(yago_class_dpb_class[yagoclass].replace("<", "").replace(">", "") + ' ' + yago_class_dpb_class[yagosuper].replace("<", "").replace(">", "")+"\n")
		if yagosuper not in yago_types:
			yago_types3.add(yagosuper)
			yago_types.add(yagosuper)
	except Exception, e:
		continue
print "finished third level"


#nt = pyRdfa(options=options).rdf_from_source('http://blog.yovisto.com/roger-cotes-and-newtons-principia-mathematica/', outputFormat='nt')
#print(nt)
#f.write(nt)
f.close()
f_out.close()