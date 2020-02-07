from unittest import TestCase

class TestImport(TestCase):
	def test_import(self):
		succ = True
		try:
			import glam_data_processing as glam
		except:
			succ = False
		self.assertTrue(succ)
