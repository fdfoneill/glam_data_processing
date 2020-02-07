import sys
import test_functionality as tfunc
import test_import as timp

try:
	funcObj = tfunc.TestFunctionality()
except:
	print("Failed to instantialize TestFunctionality(); exiting")
	sys.exit()

try:
	impObj = timp.TestImport()
except:
	print("Failed to instantialize TestImport(); exiting")
	sys.exit()

try:
	impObj.test_import()
	print("test_import: passed")
except:
	print("test_import: failed")

try:
	funcObj.test_ToDoList()
except:
	pass

try:
	funcObj.test_pullFromSourceAndImage()
	print("test_pullFromSourceAndImage: passed")
except:
	print("test_pullFromSourceAndImage: failed")

try:
	funcObj.test_pullFromS3AndImage()
	print("test_pullFromS3AndImage: passed")
except:
	print("test_pullFromS3AndImage: failed")