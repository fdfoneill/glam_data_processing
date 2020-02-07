from unittest import TestCase
import glam_data_processing as glam
import os, glob

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