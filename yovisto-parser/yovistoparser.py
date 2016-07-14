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
f = open('yovistoextract_dbpedia_filtered.txt', 'r')

f_types = open('DBPedia_types.txt', 'r')
f_out = open('yovisto_dbp_dbo.txt', 'w')

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

dbpSet = Set()
for line in f:
	dbpSet.add(line.split()[1])

for line in f_types:
	try:
		(key, value) = line.split()
	except Exception, e:
		print line.split()
		continue
	if key in dbpSet:
		f_out.write(key + ' ' + value + '\n')


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

# f_yago_class_dpb_class = open('yagoDBpediaClasses.tsv', 'r')
# yago_class_dpb_class = {}
# for line in f_yago_class_dpb_class:
# 	if line.split()[1] == 'owl:equivalentClass':
# 		yago_class_dpb_class[line.split()[0]] = line.split()[2]
# f_yago_class_dpb_class.close()

# print "finished yagoDBpediaClasses"

# f_yago_super = open('yagoTaxonomy.tsv', 'r')
# yago_super = {}
# for line in f_yago_super:
# 	if line.split()[2] == 'rdfs:subClassOf':
# 		if line.split()[1] not in f_yago_super:
# 			yago_super[line.split()[1]] = Set([line.split()[3]])
# 		else:
# 			yago_super[line.split()[1]].add(line.split()[3])
# f_yago_super.close()

# print "finished yagoDBpediaClasses" + str(len(yago_super.keys()))

# ysuperSet0 = Set()
# ysuperSet1 = Set()
# ysuperSet2 = Set()
# ysuperSet3 = Set()
# dbpSet = Set()
# for line in f:
# 	if line.split()[2] not in dbpSet:
# 		dbp_entity = line.split()[2]
# 		if dbp_entity in dbp_yago:
# 			yago_entity = dbp_yago[dbp_entity]
# 			yago_classes = yago_yago_class[yago_entity]
# 			for yclass in yago_classes:
# 				# f_out.write(dbp_entity.replace("<", "").replace(">", "") + " " + yago_class_dpb_class[yclass].replace("<", "").replace(">", "") + "\n")
# 				if yclass in yago_super:
# 					yagosuper = yago_super[yclass]
# 					for ysuper in yagosuper:
# 						f_out.write(yago_class_dpb_class[yclass].replace("<", "").replace(">", "") + " " + yago_class_dpb_class[ysuper].replace("<", "").replace(">", "") + "\n")
# 						ysuperSet0.add(yclass)
# 						ysuperSet1.add(ysuper)
# 			dbpSet.add(dbp_entity)

# print len(ysuperSet0)
# print len(ysuperSet1)

# for yclass in ysuperSet1:
# 	if yclass in yago_super and yclass not in ysuperSet0:
# 		yagosuper = yago_super[yclass]
# 		for ysuper in yagosuper:
# 			f_out.write(yago_class_dpb_class[yclass].replace("<", "").replace(">", "") + " " + yago_class_dpb_class[ysuper].replace("<", "").replace(">", "") + "\n")
# 			ysuperSet2.add(ysuper)

# print len(ysuperSet2)

# for yclass in ysuperSet2:
# 	if yclass in yago_super and yclass not in ysuperSet0 and yclass not in ysuperSet1:
# 		yagosuper = yago_super[yclass]
# 		for ysuper in yagosuper:
# 			f_out.write(yago_class_dpb_class[yclass].replace("<", "").replace(">", "") + " " + yago_class_dpb_class[ysuper].replace("<", "").replace(">", "") + "\n")
# 			ysuperSet3.add(ysuper)

# print len(ysuperSet3)







#nt = pyRdfa(options=options).rdf_from_source('http://blog.yovisto.com/roger-cotes-and-newtons-principia-mathematica/', outputFormat='nt')
#print(nt)
#f.write(nt)
f.close()
f_out.close()