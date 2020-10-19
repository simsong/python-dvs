import hashlib
import sys


filePath = (sys.argv[1])

data_file = open(filePath, 'r')

fileContents = data_file.read()
# print(type(fileContents))
# print(fileContents)
###################################################################
# splitting up file contents into data portion and header portion #
###################################################################

headerEnd = fileContents.rfind('#')
#print('\n\nLast # is at ---> ')
#print(headerEnd)

fileContents_Header = fileContents[0:headerEnd + 1]
#print('\n\nHeader portion --> ')
#print(fileContents_Header)

fileContents_Data = fileContents[headerEnd+1:]
#print('\n\nData portion --> ')
#print(fileContents_Data)

#h = hashlib.md5() # will replace with a variable in next iteration of code
#m = hashlib.md5() # will replace with a variable in next iteration of code
#d = hashlib.md5() # will replace with a variable in next iteration of code

# hashing entire file
all_at_once = hashlib.md5(fileContents.encode('utf-8'))

# hashing header portion only
headers = hashlib.md5(fileContents_Header.encode('utf-8'))

# hashing data portion only
data = hashlib.md5(fileContents_Data.encode('utf-8'))

print ('Hash Data')
print ('Complete file:', all_at_once.hexdigest())
print ('Header Section:', headers.hexdigest())
print ('Complete Data Section:', data.hexdigest())

data_file.close()
###############################
# Reading first 64KiB of file #
###############################
filePath2 = (sys.argv[1])

first64 = open(filePath2, 'r')
### read first 64KiB bytes
first64h = first64.read(65536)
### hash first 64KiB bytes of data
first64hash = hashlib.md5(first64h.encode('utf-8'))
print('First 64KiB of file hash:' , first64hash.hexdigest())
###########################################################
# Reading and hashing first 64KiB of data section of file #
###########################################################
first64data = fileContents[headerEnd+1:65536]
first64datahash = hashlib.md5(first64data.encode('utf-8'))
print('first 64KiB of data portion of file:' , first64datahash.hexdigest())
###########################################################
