# from pyRdfa import pyRdfa
# from pyRdfa.options import Options
# import xml.etree.ElementTree


# options = Options(vocab_expansion=True)
# sources = []

# e = xml.etree.ElementTree.parse('post-sitemap2.xml').getroot()

# print(e)

# for aurl in e:
#     sources.append(aurl[0].text)

f = open('yovistoextract.txt', 'r+')
f_out = open('yovistoextract_dbpedia.txt', 'w')

# i = 1

# for source in sources:
# 	nt = pyRdfa(options=options).rdf_from_source(source, outputFormat='nt')
# 	f.write(nt)
# 	print("finished " + str(i) + " of " + str(len(sources)))
# 	i += 1

for line in f:
	if '<http://www.w3.org/2004/02/skos/core#subject> <http://dbpedia.org/resource' not in line:
		continue
	print(line[:-2])
	f_out.write(line[:-2] + '\n')



#nt = pyRdfa(options=options).rdf_from_source('http://blog.yovisto.com/roger-cotes-and-newtons-principia-mathematica/', outputFormat='nt')
#print(nt)
#f.write(nt)
f.close()
f_out.close()