## set up logging
import logging, os
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
log = logging.getLogger("glam_command_line")

import argparse, glob, json, octvi, sys
import glam_data_processing as glam
from getpass import getpass
from datetime import datetime

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
		merraPassword = getpass("Merra-2 archive password:\n")
		swiUsername = input("Copernicus online username:\n")
		swiPassword = getpass("Copernicus online password:\n")
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
		mysql_pass = getpass("MySQL password:\n")
		# use input to set environent variables
		keys['glam_mysql_user'] = mysql_user
		keys['glam_mysql_pass'] = mysql_pass
	else:
		pass
	## aws
	cont = getYesNo("Set credentials for Amazon Web Services?")
	if cont:
		# get input
		awsAccess = input("AWS Access Key ID:\n")
		awsSecret = getpass("AWS Secret Access Key:\n")
		# use input to set environment variables
		keys['AWS_accessKeyId'] = awsAccess
		keys['AWS_secretAccessKey'] = awsSecret
	else:
		pass
	## save output to glam_keys.json
	cont = getYesNo("Save credentials as entered?")
	if cont:
		with open(credFile,'w') as wf:
			wf.write(json.dumps(keys))
		print(f"Keys saved to {credFile}")
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
	parser.add_argument("-ml",
		"--mask_level",
		default="ALL",
		choices=["ALL","BRAZIL","CROPMONITOR","NOMASK"],
		help="Run statistics for only a subset of crop masks")
	parser.add_argument("-al",
		"--admin_level",
		default="ALL",
		choices=["ALL","GAUL","BRAZIL"],
		help="Run statistics for only a subset of administrative regions")
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
	parser.add_argument("-id",
		"--input_directory",
		action="store",
		help="Run over a directory of existing files, rather than checking for new data")
	parser.add_argument('-u',
		"--universal",
		action='store_true',
		help="Run over all files, not just those that are flagged as missing")
	parser.add_argument('-od',
		"--output_directory",
		default=None,
		help="Save downloaded files to a directory on disk rather than deleting them.")
	parser.add_argument('-v',
		'--verbose',
		action='count',
		default=0,
		help="Display more messages; print traceback on failure")
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
	try:
		if args.universal:
			assert not args.list_missing
		elif args.list_missing:
			assert not args.universal
	except AssertionError:
		raise glam.BadInputError("--list_missing and --universal are mutually exclusive")
	if args.output_directory is not None and args.product is None:
		raise glam.BadInputError("Use of --output_directory requires that --product be set")

	## verbosity stuff
	def speak(message, cutoff = 1):
		if args.verbose >= cutoff:
			log.info(message)
		else:
			log.debug(message)
	speak(f"Running with verbosity level {args.verbose}")

	## get toDoList or directory listing
	# toDoList
	if not args.input_directory:
		missing = glam.ToDoList()
		if not args.universal:
			missing.filterUnavailable()
		downloader = glam.Downloader()
		if args.output_directory is not None:
			tempDir = args.output_directory
		else:
			tempDir = os.path.join(os.path.dirname(__file__),"temp")
		try:
			os.mkdir(tempDir)
		except FileExistsError:
			pass
	# directory listing
	else:
		dirFiles = glob.glob(os.path.join(args.input_directory,"*.tif"))
		missing = []
		for f in dirFiles:
			img = glam.getImageType(f)(f)
			missing.append((img.product,img.date,tuple([img.path])))
	try:
		j = 0
		l = len([f for f in missing])
		for f in missing:
			j += 1
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
			log.info("{0} {1}, {2} of {3}".format(*f,j,l))
			try:
				# no directory given; pull from source
				if not args.input_directory:
					if product in octvi.supported_products:
						# CHECKSUM!!!!! Current threshold for NDVI mosaic: 1GB
						pathSize = 0
						tries = 1
						sizeThreshold = 1000000000
						while pathSize < sizeThreshold: # threshold
							try:
								os.remove(paths[0])
							except:
								pass
							if tries > 3: # don't try more than three times
								raise glam.UnavailableError("File size less than 1GB after 3 tries")
							paths = downloader.pullFromSource(*f,tempDir)
							try:
								pathSize = os.path.getsize(paths[0])
								if pathSize < sizeThreshold:
									log.warning(f"File size of {pathSize} bytes below threshold")
							except IndexError:
								raise glam.UnavailableError("No file detected")
							tries += 1 # increment tries
					else:
						paths = downloader.pullFromSource(*f,tempDir)
						# check that at least one file was downloaded
						if len(paths) <1:
							raise glam.UnavailableError("No file detected")
						speak("-downloaded")
				# directory provided; use paths on disk
				else:
					paths = f[2]
				# iterate over file paths
				for p in paths:
					speak(p)
					image = glam.getImageType(p)(p)
					if (image.product == 'chirps') and (not args.stats) and (not args.ingest) and (args.mask_level=="ALL") and (args.admin_level=="ALL"):
						speak("-purging corresponding chirps-prelim product")
						try:
							glam.purge('chirps-prelim',image.date,os.environ['glam_purge_key'])
						except KeyError:
							log.warning("glam_purge_key not set. Chirps preliminary product not purged.")
					image.setStatus('downloaded',True)
					speak(f"-collection: {image.collection}",2)
					if not args.stats:
						image.ingest()
						image.setStatus('processed',True)
						speak("--ingested")
					if not args.ingest:
						image.uploadStats(crop_level=args.mask_level,admin_level=args.admin_level)
						image.setStatus('statGen',True)
						speak("--stats generated")
					if args.output_directory is None:
						os.remove(p)
						speak("--file removed")
			except glam.UnavailableError:
				log.info("(No file available)")
			except:
				if args.verbose > 0:
					log.exception("(FAILED)")
				else:
					log.error("(FAILED)")
	finally:
		if not args.input_directory and args.output_directory is None:
			for f in glob.glob(os.path.join(tempDir,"*")):
				os.remove(f)

def rectifyStats():
	parser = argparse.ArgumentParser(description="Backfill any missing statistics to database")
	parser.add_argument("-p",
		"--product",
		help="Which product to rectify")
	parser.add_argument("-d",
		"--directory",
		help="Path to directory where files of given product are stored")
	parser.add_argument("-l",
		"--list_missing",
		action="store_true",
		help="List files that have missing statistics, but do not rectify")
	parser.add_argument("-r",
		"--parallel",
		action="store_true",
		help="Use multiple cores to rectify missing stats")
	parser.add_argument("-c",
		"--cluster",
		action="store_true",
		help="Running on cluster, limit number of cores")
	args = parser.parse_args()
	# check argument validity
	try:
		if args.list_missing:
			assert not args.parallel
			assert not args.cluster
	except AssertionError:
		log.warning("'--list_missing' overrides '--parallel' and '--cluster'")
	try:
		if args.cluster:
			assert args.parallel
	except AssertionError:
		log.warning("'--parallel' not set; ignoring use of '--cluster'")
	# find and fix missing stats
	missingStats = glam.MissingStatistics(args.product)
	log.info("Fetching missing stats")
	missingStats.generate()
	if args.list_missing:
		for k in missingStats.data[args.product].keys():
			nMissing = len(missingStats.data[args.product][k])
			print(f"{args.product}, {k} | Missing {nMissing} tables")
		log.info("Done. No missing stats have been rectified")
		sys.exit()
	log.info("Rectifying all missing tables")
	missingStats.rectify(args.directory,parallel = args.parallel,cluster=args.cluster)
	log.info(f"Done. All missing stats for {args.product} have been rectified")

def fillArchive():
	parser = argparse.ArgumentParser(description="pull any missing files from S3 to local archive")
	parser.add_argument("directory",
		help="Path to directory where files of given product are stored")
	parser.add_argument("-l",
		"--list_missing",
		action='store_true',
		help="List missing files and exit without downloading")
	args = parser.parse_args()
	downloader = glam.Downloader()
	missing = downloader.listMissing(args.directory)
	l = len([t for t in missing])
	if args.list_missing:
		for t in missing:
			print(t)
		log.info("Done. Missing files not downloaded.")
	else:
		i = 0
		for t in missing:
			i += 1
			log.info(f"Pulling {t} | {i} of {l}")
			downloader.pullFromS3(*t,args.directory)
		log.info(f"Done. {args.directory} is up-to-date with S3.")

def clean():
	parser = argparse.ArgumentParser(description="Remove redundant chirps-prelim and/or NRT data")
	parser.add_argument("-c",
		"--chirps",
		action='store_true',
		help="Clean chirps-prelim data specifically")
	parser.add_argument("-n",
		"--nrt",
		action='store_true',
		help="Clean NRT NDVI data specifically")
	args = parser.parse_args()

	doChirps = args.chirps
	doNrt = args.nrt

	# if the user doesn't specify one product, assume they want both
	if (not doChirps) and (not doNrt):
		doChirps = True
		doNrt = True

	downloader = glam.Downloader()

	if doChirps:
		with downloader.engine.begin() as connection:
			latestChirps = connection.execute(f"SELECT MAX(date) FROM product_status WHERE product='chirps' AND completed=1;").fetchone()[0] # gets datetime.date object
		log.info(f"Latest Chirps file: {latestChirps.strftime('%Y-%m-%d')} / {latestChirps.strftime('%Y.%j')}")
		allPrelim = downloader.getAllS3('chirps-prelim')
		for ct in allPrelim:
			d_object = datetime.date(datetime.strptime(ct[1],"%Y-%m-%d"))
			if d_object <= latestChirps:
				glam.purge(*ct, auth_key='geoglam!23')
				log.info(f"chirps-prelim {ct[1]} <- purged")
			else:
				log.debug(f"chirps-prelim {ct[1]} <- preserved")

	if doNrt:
		with downloader.engine.begin() as connection:
			latestMod09 = connection.execute(f"SELECT MAX(date) FROM product_status WHERE product='MOD09Q1' AND completed=1;").fetchone()[0] # gets datetime.date object
			latestMyd09 = connection.execute(f"SELECT MAX(date) FROM product_status WHERE product='MYD09Q1' AND completed=1;").fetchone()[0] # gets datetime.date object
		latest8Day = max(latestMod09,latestMyd09)
		log.info(f"Latest 8-day NDVI file: {latest8Day.strftime('%Y-%m-%d')} / {latest8Day.strftime('%Y.%j')}")
		allNrt = downloader.getAllS3('MOD13Q4N')
		for nt in allNrt:
			d_object = datetime.date(datetime.strptime(nt[1],"%Y-%m-%d"))
			if d_object <= latest8Day:
				glam.purge(*nt, auth_key='geoglam!23')
				log.info(f"MOD13Q4N {nt[1]} <- purged")
			else:
				log.debug(f"MOD13Q4N {nt[1]} <- preserved")



def getInfo():
	## parse arguments
	parser = argparse.ArgumentParser(description="Get information on glam_data_processing usage and current installation")
	parser.add_argument("-v",
		"--version",
		action="version",
		help="Show glam_data_processing's version number and exit")
	parser.version = glam.__version__
	args = parser.parse_args()
	print(glam.__doc__)
	print(f"Version = {glam.__version__}")