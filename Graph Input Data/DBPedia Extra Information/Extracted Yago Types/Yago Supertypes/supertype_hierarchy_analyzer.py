import collections
from collections import defaultdict

def count_superType_level(line, typeMap, mapOfSubtypesForType):
	
	if len(mapOfSubtypesForType[line.split(' ')[0]]) == 0:
		print("No subtypes")
		typeMap[line.split(' ')[0]] = 0
		#typeMap[line.split(' ')[1][:-1]] = max(typeMap[line.split(' ')[1][:-1]],1)
		typeMap[line.split(' ')[1][:-1]] = 1
		return 0
	else:
		for subtype in mapOfSubtypesForType[line.split(' ')[0]]:
			typeMap[line.split(' ')[0]] = max(typeMap[line.split(' ')[0]], count_superType_level(subtype, typeMap, mapOfSubtypesForType)+1)
			print(" level of " + line.split(' ')[0] +  str(typeMap[line.split(' ')[0]]))
		return typeMap[line.split(' ')[0]]

print("started")
f_in = open('merge_super_types.txt', 'r')
f_out = open('yago_supertypes_filtered_sorted_count_with_hierarchy.txt', 'wb')
typeToTypeList = list(f_in)
leftTypeList = []
rightTypeList = []


for line in typeToTypeList:
	leftTypeList.append(line.split(' ')[0])
	rightTypeList.append(line.split(' ')[1][:-2])
rightTypeSet = set(rightTypeList)
leftTypeSet = set(leftTypeList)


rightTypeListCount = collections.Counter(rightTypeList)
leftTypeListCount = collections.Counter(leftTypeList)

superTypeMap = defaultdict()
mapOfSubtypesForType = defaultdict(list)
for leftType in leftTypeSet:
	mapOfSubtypesForType.setdefault(leftType, [])

for line in typeToTypeList:
	for line2 in typeToTypeList:
		rightElement = line2.split(' ')[1][:-2]
		#print("r " +rightElement)
		#print("l " +line.split(' ')[0])
		
		if rightElement == line.split(' ')[0]:
			#print("match")
			mapOfSubtypesForType[line.split(' ')[0]].append(line2)
		
for x in mapOfSubtypesForType:
    print("Supertype" + x)
    for y in mapOfSubtypesForType[x]:
        print("Sub " +y)
print("finished printing subtypes")
for leftType in leftTypeSet:
	superTypeMap.setdefault(leftType, 0)
			
for line in typeToTypeList:
	count_superType_level(line, superTypeMap, mapOfSubtypesForType)

notContainedSuperTypes = []

for superType in rightTypeSet:
	if superType not in leftTypeSet:
		notContainedSuperTypes.append(superType)

for superType in notContainedSuperTypes:
	print("Not contained super type " +superType)
	superTypeMap.setdefault(superType, 0)
	mapOfSubtypesForType.setdefault(superType, [])
	for line in typeToTypeList:
		rightElement = line.split(' ')[1][:-2]
		if rightElement == superType:
			mapOfSubtypesForType[superType].append(line)
	for line in mapOfSubtypesForType[superType]:
		sub_type = line.split(' ')[0]
		print("subtype of uncontained type named "+ superType)
		superTypeMap[superType] = max(superTypeMap[superType], superTypeMap[sub_type] +1)

for line in typeToTypeList:
	f_out.write(line[:-2] + " " + str(leftTypeListCount[line.split(' ')[0]]) + " " + str(rightTypeListCount[line.split(' ')[1][:-2]]) + " " + str(superTypeMap[line.split(' ')[0]]) + " " + str(superTypeMap[line.split(' ')[1][:-2]]) + "\r\n")
	#" " + str(superTypeMap[line.split(' ')[1]]) +


f_in.close()
f_out.close()

