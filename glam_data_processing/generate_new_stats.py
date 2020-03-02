## set up logging
import logging, os
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
log = logging.getLogger("glam_command_line")

import argparse, glob, json, subprocess, sys
import glam_data_processing as glam
from datetime import datetime

# create temporary directory for shenanigans
TEMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),"temp/")
try:
	os.mkdir(TEMP_DIR)
except FileExistsError:
	pass


def statsOneFile(file_path,admin_level, mask_level) -> bool:
	base = os.path.basename(file_path)
	log.info(f"Processing {base}")
	try:
		img = glam.getImageType(file_path)(file_path) # create Image object
		img.uploadStats(admin_level=admin_level,crop_level=mask_level)
		return True
	except:
		log.exception(f"FAILED in processing {base}")
		return False


def download_and_statsOneFile(product,date,directory,admin_level,mask_level,save=False) -> bool:
	"""Wrapper for statsOneFile in case downloading is required"""
	paths = ()
	try:
		log.info(f"Downloading {product} {date}")
		downloader = glam.Downloader()
		paths = downloader.pullFromS3(product,date,directory)
		log.info(f"Processing {product} {date}")
		for p in paths:
			assert statsOneFile(file_path=p,admin_level=admin_level,mask_level=mask_level)
		downloader = None
		return True
	except:
		log.exception(f"{product} {date} FAILED")
		return False
	finally:
		if not save:
			for p in paths:
				os.remove(p)


def getAllTiffs(in_dir) -> list:
	"""Takes input directory path, returns list of all tiff full paths"""
	return glob.glob(os.path.join(in_dir,"*.tif"))


def getProductDateTuple(file_path) -> tuple:
	"""extracts the product and date from a file path; returns as a tuple"""
	try:
		base = os.path.basename(file_path)
		parts= base.split(".")
		product = parts[0]
		try:
			date = datetime.strptime(f"{parts[1]}.{parts[2]}","%Y.%j").strftime("%Y-%m-%d")
		except:
			date = datetime.strptime(parts[1],"%Y-%m-%d").strftime("%Y-%m-%d")
		return (product,date)
	except:
		log.exception(f"Failed to extract product-date tuple for {os.path.basename(file_path)}")


def main():
	## parse arguments
	parser = argparse.ArgumentParser(description="Generate new statistics, in parallel")
	parser.add_argument("file_directory",
		help="Directory where imagery files are / will be stored")
	parser.add_argument("-al",
		"--admin_level",
		default="ALL",
		choices=["ALL","GAUL","BRAZIL"],
		help="Run statistics for only a subset of administrative regions")
	parser.add_argument("-ml",
		"--mask_level",
		default="ALL",
		choices=["ALL","BRAZIL","CROPMONITOR","NOMASK"],
		help="Run statistics for only a subset of crop masks")
	parser.add_argument("-c",
		"--cores",
		required=True,
		help="How many cores to use")
	parser.add_argument("-l",
		"--logfile",
		help="Where to write output log messages")
	parser.add_argument("-d",
		"--download_files",
		action='store_true',
		help="Download any files not currently on disk. Pull them from S3 before running stats. Requires product flag.")
	parser.add_argument('-p',
		"--product",
		help="Which product to pull. Required if --download_files is set.")
	parser.add_argument("-s",
		"--save_results",
		action='store_true',
		help="If set, does not delete downloaded files")
	parser.add_argument("-mo",
		"--missing_only",
		action='store_true',
		help="Generate statistics only for files NOT currently on disk in file_directory")
	parser.add_argument("-META",
		metavar='metastring',
		default = None,
		action='store',
		help="Do not set this flag. For internal use only.")

	args = parser.parse_args()

	## META stores file path or product/date as json
	if args.META:
		meta_parts = args.META.split(".")
		if args.download_files:
			product = meta_parts[0]
			date = meta_parts[1]
			print(product)
			print(date)
			#download_and_statsOneFile(product,date,args.file_directory,args.admin_level,args.mask_level,args.save_results)
		else:
			in_file = args.META
			print(in_file)
			#statsOneFile(in_file, args.admin_level,args.mask_level)
		sys.exit()
		

	## confirm argument validity
	try:
		if args.download_files:
			assert args.product
	except AssertionError:
		log.error("If download_files is set, a product must be specified.")
		sys.exit()
	if args.mask_level == "BRAZIL" and args.admin_level == "GAUL":
		log.error("Can't generate Brazil crop stats for Gaul regions")
		sys.exit()
	if not args.download_files:
		try:
			assert not args.save_results
		except AssertionError:
			log.warning("--download_files not set; --save_results flag ignored")
		try:
			assert not args.missing_only
		except AssertionError:
			log.warning("--download_files not set; --missing_only flag ignored")
	if not args.logfile:
		args.logfile = os.path.join(TEMP_DIR,"gns_log.txt")

	lines = []
	## perform stats on existing files
	extant = getAllTiffs(args.file_directory)
	if not args.missing_only:
		for f in extant:
			line = f"python {__file__} {args.file_directory} -al {args.admin_level} -ml {args.mask_level} -c {args.cores} -META '{f}'"
			if args.save_results:
				line += " -s" 
			line += "\n"
			lines += line
		

	## download and process new files if requested
	if args.download_files:
		downloader= glam.Downloader()
		onDisk = [getProductDateTuple(f) for f in extant]
		#available = downloader.getAllS3(product=args.product)
		#missing = [t for t in available if t not in onDisk]
		missing = downloader.listMissing(args.file_directory)
		if len(missing) == 0:
			log.info(f"No missing files; all available files on S3 are also in {args.file_directory}")
		else:
			for t in missing:
				product,date=t
				meta = f"{product}.{date}"
				line = f"python {__file__} {args.file_directory} -al {args.admin_level} -ml {args.mask_level} -c {args.cores} -d -META {meta}"
				if args.save_results:
					line += " -s" 
				line += " \n"
				lines += line 

			downloader=None
	command_file = os.path.join(TEMP_DIR,"gns_commands.txt")
	with open(command_file,'w') as wf:
		wf.writelines(lines)
	shell_call = ["nohup","sh","-c", f'"cat {command_file} | parallel -j {args.cores}"', "&>",args.logfile,"&"]
	print(" ".join(shell_call))
	#subprocess.call(shell_call)
	#os.remove(command_file)


if __name__ == "__main__":
	main()