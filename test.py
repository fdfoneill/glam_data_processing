from unittest import TestCase
import os, glob, logging

class TestImport(TestCase):
	def test_import(self):
		succ = True
		try:
			import glam_data_processing as glam
		except:
			succ = False
		self.assertTrue(succ)

import glam_data_processing as glam

class TestFunctionality(TestCase):
	def test_ToDoList(self):
		failure = False
		try:
			missing = glam.ToDoList()
			self.assertIsInstance(missing,glam.ToDoList)
			missing.filterUnavailable()
		except:
			failure = True
		self.assertFalse(failure)

	def test_pullFromSourceAndImage(self):
		failure = False
		fileTuples = []
		knownValids = [
				('chirps','2019-12-01'),
				('merra-2','2019-12-25'),
				('swi','2019-01-23')
				]
		try:
			downloader = glam.Downloader()
			
			for t in knownValids:
				#print(t[0])
				fileTuples.append(downloader.pullFromSource(*t,os.path.join(os.path.dirname(__file__),"temp")))
			for t in fileTuples:
				for f in t:
					img = glam.getImageType(f)(f)
					self.assertTrue(img.isProcessed())
					self.assertTrue(img.statsGenerated())
		except:
			failure = True
		finally:
			for f in glob.glob(os.path.join(os.path.dirname(__file__),"temp/*")):
				os.remove(f)
		self.assertFalse(failure)

	def test_pullFromS3AndImage(self):
		#print(__file__)
		failure = False
		fileTuples = []
		knownValids = [
				('chirps','2019-12-01'),
				('merra-2','2019-12-25'),
				('swi','2019-01-23')
				]
		try:
			downloader = glam.Downloader()
			
			for t in knownValids:
				#print(t[0])
				fileTuples.append(downloader.pullFromS3(*t,os.path.join(os.path.dirname(__file__),"temp")))
			for t in fileTuples:
				for f in t:
					img = glam.getImageType(f)(f)
					self.assertTrue(img.isProcessed())
					self.assertTrue(img.statsGenerated())
		except Exception as e:
			print(e)
			failure = True
		finally:
			for f in glob.glob(os.path.join(os.path.dirname(__file__),"temp/*")):
				os.remove(f)
		self.assertFalse(failure)

	def test_pullModisFromS3AndImage(self):
		failure = False
		fileTuple = None
		#print(__file__)
		try:
			downloader = glam.Downloader()
			fileTuple = downloader.pullFromS3("MOD09Q1","2019-01-01",os.path.join(os.path.dirname(__file__),"temp"))
			img = glam.getImageType(fileTuple[0])(fileTuple[0])
			self.assertTrue(img.isProcessed())
			self.assertTrue(img.statsGenerated())
		except:
			logging.exception("FAILURE")
			failure = True
		finally:
			if fileTuple:
				os.remove(fileTuple[0])
		self.assertFalse(failure)


def main():
	res = [0,0]
	try:
		funcObj = TestFunctionality()
	except:
		print("Failed to instantialize TestFunctionality(); exiting")
		sys.exit()

	try:
		impObj = TestImport()
	except:
		print("Failed to instantialize TestImport(); exiting")
		sys.exit()

	try:
		impObj.test_import()
		print("test_import: PASSED")
		res[0] += 1
	except:
		print("test_import: FAILED")
		res[1] +=1

	try:
		funcObj.test_ToDoList()
		print("test_ToDoList: PASSED")
		res[0] += 1
	except:
		print("test_ToDoList: FAILED")
		res[1] +=1

	try:
		funcObj.test_pullFromSourceAndImage()
		print("test_pullFromSourceAndImage: PASSED")
		res[0] += 1
	except:
		print("test_pullFromSourceAndImage: FAILED")
		res[1] +=1

	try:
		funcObj.test_pullFromS3AndImage()
		print("test_pullFromS3AndImage: PASSED")
		res[0] += 1
	except:
		print("test_pullFromS3AndImage: FAILED")
		res[1] +=1

	try:
		funcObj.test_pullModisFromS3AndImage()
		print("test_pullModisFromS3AndImage: PASSED")
		res[0] += 1
	except:
		logging.exception("FAILURE")
		print("test_pullModisFromS3AndImage: FAILED")
		res[1] +=1
	print("Ran {0} tests | Successes: {1} | Failures: {2}".format(sum(res),*res))

if __name__ == "__main__":
	main()