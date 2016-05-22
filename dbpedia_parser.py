filename = 'instance_types_en.ttl'
with open('en_DBPedia_types.ttl', 'w', encoding="utf8") as outfile:
    with open(filename, 'r', encoding="utf8") as infile:
       for line in infile:
        	substring = '<http://www.w3.org/2002/07/owl#Thing>'
        	positionOfSubstring = line.find(substring)
        	if (positionOfSubstring==-1):
        		outfile.write(line)
print("done")
        	

