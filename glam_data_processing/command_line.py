import os
import glam_data_processing as glam

def getYesNo(message:str) -> bool:
	cont = input(message+"[Y/N]\n")
	if cont.lower() in ("y","yes"):
		return True
	elif cont.lower() in ("n","no"):
		return False
	else:
		print("Error: Input not recognized. Please select one of: [Y/N]")
		return getYesNo(message)	

def setCredentials():
	## get existing credentials
	credDir = os.path.dirname(os.path.dirname(__file__))
	credFile = os.path.join(credDir,"keys.json")
	try:
		with open(credFile,'r') as rf:
			keys = json.loads(rf.read())
	except FileNotFoundError:
		keys = {}
	## data archive
	cont = getYesNo("Set credentials for the MERRA-2 archive and Copernicus online?")
	if cont
		# get input
		merraUsername = input("Merra-2 archive username:\n")
		merraPassword = input("Merra-2 archive password:\n")
		swiUsername = input("Copernicus online username:\n")
		swiPassword = input("Copernicus online password:\n")
		# use input to set environent variables
		keys['merrausername'] = merraUsername
		keys['merrapassword'] = merraPassword
		keys['swiusername'] = swiUsername
		keys['swipassword'] = swiPassword
	else
		pass
	## database
	cont = getYesNo("Set credentials for database?")
	if cont:
		# get input
		mysql_user = input("MySQL username:\n")
		mysql_pass = input("MySQL password:\n")
		# use input to set environent variables
		keys['glam_mysql_user'] = mysql_user
		keys['glam_mysql_pass'] = mysql_pass
	else:
		pass
	## save output to keys.json
	cont = getYesNo("Save credentials as entered?")
	if cont:
		with open(credFile,'w') as wf:
			wf.write(json.dumps(keys))
	else:
		print("Keys not saved.")
	sys.exit()

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