class BadInputError(Exception):
	def __init__(self, data):
		self.data = data
	def __str__(self):
		return repr(self.data)

class UploadFailureError(Exception):
	def __init__(self, data):
		self.data = data
	def __str__(self):
		return repr(self.data)

class UnavailableError(Exception):
	def __init__(self,data):
		self.data=data
	def __str__(self):
		return repr(self.data)

class RecordNotFoundError(Exception):
	def __init__(self,data):
		self.data=data
	def __str__(self):
		return repr(self.data)

class NoCredentialsError(Exception):
	def __init__(self,data):
		self.data=data
	def __str__(self):
		return repr(self.data)
