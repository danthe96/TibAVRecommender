import sys
filename2 = 'gnd_dedbpedia_filtered.ttl'
filename = 'de_en.ttl'
with open('gnd_endbpedia.ttl', 'w', encoding="utf8") as outfile:
	with open(filename2, 'r', encoding="utf8") as infile:
	
		for line in infile:
			substring = '> '
			positionOfSubstring = line.find(substring)
			entity = line[positionOfSubstring +2:len(line)-1]
			i=0
			"""while (i<len(entity)):
				print(str(i) + " " + entity[i] + "\n")
				i+=1"""
			#print(entity)
			#entityEncoded = entity.encode('utf8')
			#sys.stdout.buffer.write(entityEncoded )
			if (positionOfSubstring !=-1):
				with open (filename, 'r', encoding="utf8") as infile2:
					for line2 in infile2:
						
						positionOfEntity = line2.find(entity)
						line22 = "line1 " + entity + " line2 "+ line2 +"\n"
						
						line22Encoded = line22.encode('utf8')
						#sys.stdout.buffer.write(line22Encoded )
						#print("line1 " + entity + " line2 "+ line2 +"\n")
						if (positionOfEntity!=-1):
							string2 = "match " + entity +"\n"
						
							string2encoded = string2.encode('utf8')
							#sys.stdout.buffer.write(string2encoded )
							outputString =line[0:positionOfSubstring+1] + " " + line2[positionOfEntity + len(entity)+1:len(line2)]
							#outputEncoded = outputString.encode('utf8')
							#sys.stdout.buffer.write(outputEncoded)
							
							outfile.write(outputString)
							break
					infile2.close()
print("done")
        	

