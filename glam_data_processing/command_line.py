## set up logging
import logging, os
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
log = logging.getLogger("glam_command_line")

import argparse, glob, json, octvi, sys
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
	credFile = os.path.join(credDir,"glam_keys.json")
	try:
		with open(credFile,'r') as rf:
			keys = json.loads(rf.read())
	except FileNotFoundError:
		keys = {}
	## data archive
	cont = getYesNo("Set credentials for the MERRA-2 archive and Copernicus online?")
	if cont:
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
	else:
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
	## save output to glam_keys.json
	cont = getYesNo("Save credentials as entered?")
	if cont:
		with open(credFile,'w') as wf:
			wf.write(json.dumps(keys))
	else:
		print("Keys not saved.")
	sys.exit()

def updateData():
	## parse arguments
	parser = argparse.ArgumentParser(description="Update GLAM system imagery data")
	parser.add_argument("-a",
		"--ancillary",
		action='store_true',
		help="Only do ancillary data, not NDVI")
	parser.add_argument("-n",
		"--ndvi",
		action='store_true',
		help="Only do NDVI data, not ancillary")
	parser.add_argument('-p',
		'--product',
		default=None,
		required=False,
		choices=octvi.supported_products+glam.ancillary_products,
		help="Only update the specified product")
	parser.add_argument("-i",
		"--ingest",
		action='store_true',
		help="Ingest only, no stats generation")
	parser.add_argument('-s',
		"--stats",
		action='store_true',
		help="Stats generation only, no ingest")
	parser.add_argument('-l',
		'--list_missing',
		action='store_true',
		help="Print list of missing imagery; do not download, ingest, or generate statistics")
	parser.add_argument('-v',
		'--verbose',
		action='store_true',
		help="Display traceback on failure")
	args = parser.parse_args()

	## confirm exclusivity
	try:
		if args.ancillary:
			assert not args.ndvi
			assert not args.product
		elif args.ndvi:
			assert not args.ancillary
			assert not args.product
		elif args.product:
			assert not args.ancillary
			assert not args.ndvi
	except AssertionError:
		raise glam.BadInputError("--ancillary, --product, and --ndvi are mutually exclusive")
	try:
		if args.ingest:
			assert not args.stats
		elif args.stats:
			assert not args.ingest
	except AssertionError:
		raise glam.BadInputError("--ingest and --stats are mutually exclusive")
	## get toDoList
	missing = glam.ToDoList()
	missing.filterUnavailable()
	downloader = glam.Downloader()
	tempDir = os.path.join(os.path.dirname(__file__),"temp")
	try:
		os.mkdir(tempDir)
	except FileExistsError:
		pass
	try:
		for f in missing:
			product = f[0]
			if product in octvi.supported_products and args.ancillary:
				continue
			if product in glam.ancillary_products and args.ndvi:
				continue
			if args.product and product != args.product:
				continue
			if args.list_missing:
				print("{0} {1}".format(*f))
				continue
			log.info("{0} {1}".format(*f))
			try:
				if not downloader.isAvailable(*f):
					raise glam.UnavailableError("No file detected")
				paths = downloader.pullFromSource(*f,tempDir)
				# check that at least one file was downloaded
				if len(paths) <1:
					raise glam.UnavailableError("No file detected")
				if args.verbose:
					log.info("-downloaded")
				else:
					log.debug("-downloaded")
				# iterate over file paths
				for p in paths:
					if args.verbose:
						log.info(p)
					else:
						log.debug(p)
					image = glam.Image(p)
					if image.product == 'chirps':
						if args.verbose:
							log.info("-purging corresponding chirps-prelim product")
						else:
							log.debug("-purging corresponding chirps-prelim product")
						try:
							glam.purge('chirps-prelim',image.date,os.environ['glam_purge_key'])
						except KeyError:
							log.warning("glam_purge_key not set. Chirps preliminary product not purged.")
					image.setStatus('downloaded',True)
					if args.verbose:
						log.info(f"-collection: {image.collection}")
					else:
						log.debug(f"-collection: {image.collection}")
					if not args.stats:
						image.ingest()
						image.setStatus('processed',True)
						if args.verbose:
							log.info("--ingested")
						else:
							log.debug("--ingested")
					if not args.ingest:
						image.uploadStats()
						image.setStatus('statGen',True)
						if args.verbose:
							log.info("--stats generated")
						else:
							log.debug("--stats generated")
					os.remove(p)
					if args.verbose:
						log.info("--file removed")
					else:
						log.debug("--file removed")
			except glam.UnavailableError:
				log.info("(No file available)")
			except:
				if args.verbose:
					log.exception("(FAILED)")
				else:	
					log.error("(FAILED)")
	finally:
		for f in glob.glob(os.path.join(tempDir,"*")):
			os.remove(f)

def getInfo():
	## parse arguments
	parser = argparse.ArgumentParser(description="Get information on glam_data_processing usage and current installation")
	parser.add_argument("-v",
		"--version",
		action="store_true",
		help="Only print version number")
	args = parser.parse_args()
	if not args.version:
		print(glam.__doc__)
	else:
		print("glam_data_processing")
	print(f"Version = {glam.__version__}")