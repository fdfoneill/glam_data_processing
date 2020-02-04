import os
import glam_data_processing as glam

def setCredentials():
	## data archive
	cont = input("Set credentials for the MERRA-2 archive and Copernicus online? [Y/N]\n")
	if cont.lower() in ("y","yes"):
		# get input
		merraUsername = input("Merra-2 archive username:\n")
		merraPassword = input("Merra-2 archive password:\n")
		swiUsername = input("Copernicus online username:\n")
		swiPassword = input("Copernicus online password:\n")
		# use input to set environent variables
		os.environ['merrausername'] = merraUsername
		os.environ['merrapassword'] = merraPassword
		os.environ['swiusername'] = swiUsername
		os.environ['swipassword'] = swiPassword
	elif cont.lower() in ("n","no"):
		pass
	else:
		print("Error: Input not recognized. Please select one of: [Y/N]")
		setCredentials()
	## database
	cont = input("Set credentials for database? [Y/N]\n")
	if cont.lower() in ("y","yes"):
		# get input
		mysql_user = input("MySQL username:\n")
		mysql_pass = input("MySQL password:\n")
		# use input to set environent variables
		os.environ['glam_mysql_user'] = mysql_user
		os.environ['glam_mysql_pass'] = mysql_pass
	elif cont.lower() in ("n","no"):
		pass
	else:
		print("Error: Input not recognized. Please select one of: [Y/N]")
		setCredentials()

def updateData():
	## get toDoList
	missing = glam.ToDoList()
	downloader = glam.Downloader()
	tempDir = os.path.dirname(__file__)
	for f in missing:
		log.info("{0} {1}".format(*f))
		try:
			if not downloader.isAvailable(*f):
				raise glam.UnavailableError("No file detected")
			paths = downloader.pullFromSource(*f,tempDir)
			# check that at least one file was downloaded
			if len(paths) <1:
				raise glam.UnavailableError("No file detected")
			log.debug("-downloaded")
			# iterate over file paths
			for p in paths:
				log.debug(p)
				image = glam.Image(p)
				if image.product == 'chirps':
					log.debug("-purging corresponding chirps-prelim product")
					try:
						glam.purge('chirps-prelim',image.date,os.environ['glam_purge_key'])
					except KeyError:
						log.warning("glam_purge_key not set. Chirps preliminary product not purged.")
				image.setStatus('downloaded',True)
				log.debug(f"-collection: {image.collection}")
				image.ingest()
				image.setStatus('processed',True)
				log.debug("--ingested")
				image.uploadStats()
				image.setStatus('statGen',True)
				log.debug("--stats generated")
				os.remove(p)
				log.debug("--file removed")
		except UnavailableError:
			log.info("(No file available)")