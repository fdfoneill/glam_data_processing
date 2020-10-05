## set up logging
import logging, os
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
log = logging.getLogger(__name__)

import argparse, copy, copyreg, glob, multiprocessing, rasterio, shutil, subprocess, types
from datetime import datetime
from rasterio.windows import Window
from rasterio.io import MemoryFile
from functools import partial
from octvi import supported_products
import numpy as np
import glam_data_processing as glam


def getSwiBaselineDoy(new_img:glam.Image) -> int:
	for valid_swi_date in range(1,365,5):
		if abs(int(new_img.doy)-valid_swi_date) <= 2:
			return valid_swi_date
			

def getInputPathList(new_img:glam.Image) -> list:
	data_directory = os.path.dirname(new_img.path)
	product = new_img.product
	input_images = []

	allFiles = glob.glob(os.path.join(data_directory,"*.tif"))
	if len(allFiles) < 1:
		log.error(f"Failed to collect archive from {data_directory}")
		return []

	for f in allFiles:
		try:
			img = glam.getImageType(f)(f)
		except: # not a well-formed image; for example; an intermediate image
			continue
		if product in supported_products+["merra-2"]:
			# we can just use DOY for NDVI products and merra (since merra is daily)
			if img.doy == doy:
				input_images.append(img)
		elif product == "swi":
			output_doy = getSwiBaselineDoy(new_img)
			if abs(int(img.doy)-output_doy) <= 2: # 'img' is the closest date from given year to eventual output date
				input_images.append(img)
		elif product == "chirps": # must be chirps
			if img.date.split("-")[1:] == new_image.date.split("-")[1:]: # tests that month and day are equal
				input_images.append(img)
		else:
			log.error(f"Product {product} not recognized in getInputPathList()")
	
	# sort input_images
	input_images.sort()
	input_images.reverse()
	input_paths = [i.path for i in input_images]

	return input_paths


def cloud_optimize_inPlace(in_file:str) -> None:
	"""Takes path to input and output file location. Reads tif at input location and writes cloud-optimized geotiff of same data to output location."""
	## add overviews to file
	cloudOpArgs = ["gdaladdo",in_file]
	subprocess.call(cloudOpArgs)

	## copy file
	intermediate_file = in_file.replace(".tif",".TEMP.tif")
	with open(intermediate_file,'wb') as a:
		with open(in_file,'rb') as b:
			shutil.copyfileobj(b,a)

	## add tiling to file
	cloudOpArgs = ["gdal_translate",intermediate_file,in_file,'-q','-co', "TILED=YES",'-co',"COPY_SRC_OVERVIEWS=YES",'-co', "COMPRESS=LZW", "-co", "PREDICTOR=2"]
	if product in supported_products:
		cloudOpArgs.append("-co")
		cloudOpArgs.append("BIGTIFF=YES")
	subprocess.call(cloudOpArgs)

	## remove intermediate
	os.remove(intermediate_file)


def anomaly_ingest(input_tuple:tuple):
	f, y = input_tuple
	anom = glam.AnomalyBaseline(f)
	anom.year = y
	anom.ingest()

if __name__ == "__main__":

	startTime = datetime.now()

	parser = argparse.ArgumentParser(description="Update GLAM system imagery data")
	parser.add_argument("input_file",
		type=str,
		help="Path to new file to be added to anomaly baselines"
		)
	parser.add_argument("-n",
		"--n_workers",
		type=int,
		default=20,
		help="Number of parallel processes to run"
		)
	args = parser.parse_args()

	log.debug("Parsing input image")
	new_image = glam.getImageType(args.input_file)(args.input_file)
	n_workers=args.n_workers

	# extract directory, product, and doy from image object
	data_directory = os.path.dirname(new_image.path)
	product=new_image.product
	doy=new_image.doy

	# set baseline_locations
	baseline_root = os.path.join("/gpfs","data1","cmongp2","GLAM","anomaly_baselines",product)
	baseline_locations = {anomaly_type:os.path.join(baseline_root,anomaly_type) for anomaly_type in ["mean_5year","median_5year",'mean_10year','median_10year','mean_full','median_full']}
	
	# get input paths
	input_paths = getInputPathList(new_image)

	# get input raster metadata and dimensions
	getmeta = rasterio.open(new_image.path)
	metaprofile = getmeta.profile
	hnum = getmeta.width
	vnum = getmeta.height
	del getmeta

	# add BIGTIFF where necessary
	if product in supported_products:
		metaprofile['BIGTIFF'] = 'YES'

	def _mp_serf(targetwindow,input_paths,dtype='int16') -> tuple:
		"""Worker function for use with multiprocessing

		Returns a tuple of targetwindow and outputstore;
		outputstore holds a dictionary of calculated means/medians
		for the targetwindow, with the following keys:
			* mean_5year
			* median_5year
			* mean_10year
			* median_10year
			* mean_full
			* median_full

		***

		Parameters
		----------
		targetwindow:rasterio window
		input_paths:list
			Ordered list of filepaths

		"""

		# track how many years we have looped over for the anomaly
		outputstore = {}
		valuestore = []
		yearscounter = 0
		# Read an input block
		for inputfile in input_paths:
			# inputfile = img.path
			inputhandle = rasterio.open(inputfile, 'r')
			thiswindowdata = inputhandle.read(1, window=targetwindow)
			valuestore += [thiswindowdata]
			yearscounter += 1
			# If it's a benchmarking year (5, 10, or final) then write something to the
			# output handle and close it
			if yearscounter == 5:
				# Calculate the 5-year mean and write it
				data_5yr = np.array(valuestore)
				mean_5yr_calc = np.ma.average(data_5yr, axis=0, weights=((data_5yr >= -1000) * (data_5yr <= 10000)))
				mean_5yr_calc[mean_5yr_calc.mask==True] = -3000
				outputstore['mean_5year'] = mean_5yr_calc.astype(dtype)
				del mean_5yr_calc

				# Calculate the 5-year median and write it
				median_5yr_masked = np.ma.masked_outside(data_5yr, -1000, 10000)
				median_5yr_calc = np.ma.median(median_5yr_masked, axis=0)
				median_5yr_calc[median_5yr_calc.mask==True] = -3000
				outputstore['median_5year'] = median_5yr_calc.astype(dtype)
				del data_5yr, median_5yr_masked, median_5yr_calc

			if yearscounter == 10:
				# Calculate the 10-year mean and write it
				data_10yr = np.array(valuestore)
				mean_10yr_calc = np.ma.average(data_10yr, axis=0, weights=((data_10yr >= -1000) * (data_10yr <= 10000)))
				mean_10yr_calc[mean_10yr_calc.mask==True] = -3000
				outputstore['mean_10year'] = mean_10yr_calc.astype(dtype)
				del mean_10yr_calc

				# Calculate the 10-year median and write it
				median_10yr_masked = np.ma.masked_outside(data_10yr, -1000, 10000)
				median_10yr_calc = np.ma.median(median_10yr_masked, axis=0)
				median_10yr_calc[median_10yr_calc.mask==True] = -3000
				outputstore['median_10year'] = median_10yr_calc.astype(dtype)
				del data_10yr, median_10yr_masked, median_10yr_calc

			if yearscounter == len(input_paths):
				# Calculate the full time series mean and write it
				data_full = np.array(valuestore)
				mean_full_calc = np.ma.average(data_full, axis=0, weights=((data_full >= -1000) * (data_full <= 10000)))
				mean_full_calc[mean_full_calc.mask==True] = -3000
				outputstore['mean_full'] = mean_full_calc.astype(dtype)
				del mean_full_calc

				# Calculate the full time series median and write it
				median_full_masked = np.ma.masked_outside(data_full, -1000, 10000)
				median_full_calc = np.ma.median(median_full_masked, axis=0)
				median_full_calc[median_full_calc.mask==True] = -3000
				outputstore['median_full'] = median_full_calc.astype(dtype)
				del data_full, median_full_masked, median_full_calc
			inputhandle.close()
		return(targetwindow, outputstore)

	# since mp.map() only allows one argument, we have to make a function with only a single parameter...
	def _mp_worker(targetwindow):
		return _mp_serf(targetwindow,input_paths,metaprofile['dtype'])

	log.debug("Starting baseline update")

	# set output filenames
	if product in supported_products + ["merra-2"]: # can just use doy
		mean_5yr_name = os.path.join(baseline_locations["mean_5year"], f"{product}.{doy}.anomaly_mean_5year.tif")
		median_5yr_name = os.path.join(baseline_locations["median_5year"], f"{product}.{doy}.anomaly_median_5year.tif")
		mean_10yr_name = os.path.join(baseline_locations["mean_10year"], f"{product}.{doy}.anomaly_mean_10year.tif")
		median_10yr_name = os.path.join(baseline_locations["median_10year"],f"{product}.{doy}.anomaly_median_10year.tif")
		mean_full_name = os.path.join(baseline_locations["mean_full"], f"{product}.{doy}.anomaly_mean_full.tif")
		median_full_name = os.path.join(baseline_locations["median_full"], f"{product}.{doy}.anomaly_median_full.tif")
	elif product == "chirps":
		mean_5yr_name = os.path.join(baseline_locations["mean_5year"], f"{product}.{new_image.date}.anomaly_mean_5year.tif")
		median_5yr_name = os.path.join(baseline_locations["median_5year"], f"{product}.{new_image.date}.anomaly_median_5year.tif")
		mean_10yr_name = os.path.join(baseline_locations["mean_10year"], f"{product}.{new_image.date}.anomaly_mean_10year.tif")
		median_10yr_name = os.path.join(baseline_locations["median_10year"],f"{product}.{new_image.date}.anomaly_median_10year.tif")
		mean_full_name = os.path.join(baseline_locations["mean_full"], f"{product}.{new_image.date}.anomaly_mean_full.tif")
		median_full_name = os.path.join(baseline_locations["median_full"], f"{product}.{new_image.date}.anomaly_median_full.tif")
	elif product == "swi":
		output_doy = str(getSwiBaselineDoy(new_image)).zfill(3)
		mean_5yr_name = os.path.join(baseline_locations["mean_5year"], f"{product}.{output_doy}.anomaly_mean_5year.tif")
		median_5yr_name = os.path.join(baseline_locations["median_5year"], f"{product}.{output_doy}.anomaly_median_5year.tif")
		mean_10yr_name = os.path.join(baseline_locations["mean_10year"], f"{product}.{output_doy}.anomaly_mean_10year.tif")
		median_10yr_name = os.path.join(baseline_locations["median_10year"],f"{product}.{output_doy}.anomaly_median_10year.tif")
		mean_full_name = os.path.join(baseline_locations["mean_full"], f"{product}.{output_doy}.anomaly_mean_full.tif")
		median_full_name = os.path.join(baseline_locations["median_full"], f"{product}.{output_doy}.anomaly_median_full.tif")
	else:
		log.error(f"Product {product} not recognized for output baseline file name generation")

	# # open input MemoryFiles
	# self.open()

	# open output handles
	log.debug("Opening handles")
	mean_5yr_handle = rasterio.open(mean_5yr_name, 'w', **metaprofile)
	median_5yr_handle = rasterio.open(median_5yr_name, 'w', **metaprofile)
	mean_10yr_handle = rasterio.open(mean_10yr_name, 'w', **metaprofile)
	median_10yr_handle = rasterio.open(median_10yr_name, 'w', **metaprofile)
	mean_full_handle = rasterio.open(mean_full_name, 'w', **metaprofile)
	median_full_handle = rasterio.open(median_full_name, 'w', **metaprofile)

	# get windows
	log.debug("Getting windows")
	blocksize = 256
	windows = []
	for hstart in range(0, hnum, blocksize):
		for vstart in range(0, vnum, blocksize):
			hwin = blocksize
			vwin = blocksize
			if ((hstart + blocksize) > hnum):
				hwin = (hnum % blocksize)
			if ((vstart + blocksize) > vnum):
				vwin = (vnum % blocksize)
			targetwindow = Window(hstart, vstart, hwin, vwin)
			# if ((hstart + blocksize) > hnum) or ((vstart + blocksize) > vnum):
			# 	targetwindow = Window(hstart, vstart, (hnum % blocksize), (vnum % blocksize))
			windows += [targetwindow]

	# do multiprocessing
	log.info(f"Processing ({new_image.product} {new_image.date}) | {n_workers} parallel processes")
	parallelStartTime = datetime.now()
	p = multiprocessing.Pool(n_workers)

	for win, values in p.imap(_mp_worker, windows):
		mean_5yr_handle.write(values['mean_5year'], window=win, indexes=1)
		median_5yr_handle.write(values['median_5year'], window=win, indexes=1)
		mean_10yr_handle.write(values['mean_10year'], window=win, indexes=1)
		median_10yr_handle.write(values['median_10year'], window=win, indexes=1)
		mean_full_handle.write(values['mean_full'], window=win, indexes=1)
		median_full_handle.write(values['median_full'], window=win, indexes=1)

	## close pool
	p.close()
	p.join()

	# ## close MemoryFiles
	# self.close()

	## close handles
	mean_5yr_handle.close()
	median_5yr_handle.close()
	mean_10yr_handle.close()
	median_10yr_handle.close()
	mean_full_handle.close()
	median_full_handle.close()

	log.info(f"Finished parallel processing in {datetime.now() - parallelStartTime}")

	# cloud-optimize new anomalies
	log.debug("Converting baselines to cloud-optimized geotiffs")
	cogStartTime = datetime.now()

	output_paths = [mean_5yr_name, median_5yr_name, mean_10yr_name, median_10yr_name, mean_full_name, median_full_name]

	p = multiprocessing.Pool(len(output_paths))
	p.imap(cloud_optimize_inPlace,output_paths)

	# for f in [mean_5yr_name, median_5yr_name, mean_10yr_name, median_10yr_name, mean_full_name, median_full_name]:
	# 	cloud_optimize_inPlace(f)

	log.info(f"Finished cloud-optimizing in {datetime.now() - cogStartTime}")

	# ingest new anomalies
	log.debug("Ingesting updated anomaly baselines to AWS")
	ingestStartTime = datetime.now()
	arg_tuples= [(x,new_image.year) for x in output_paths]
	p.imap(anomaly_ingest,arg_tuples)

	log.info(f"Finished ingesting in {datetime.now() - ingestStartTime}")

	# for f in [mean_5yr_name, median_5yr_name, mean_10yr_name, median_10yr_name, mean_full_name, median_full_name]:
	# 	anom = glam.AnomalyBaseline(f)
	# 	anom.year = new_image.year
	# 	anom.ingest()

	## close pool
	p.close()
	p.join()

	endTime = datetime.now()
	log.info(f"Finished in {endTime-startTime}")


# def main():
# 	parser = argparse.ArgumentParser(description="Update GLAM system imagery data")
# 	parser.add_argument("input_file",
# 		type=str,
# 		help="Path to new file to be added to anomaly baselines"
# 		)
# 	parser.add_argument("-n",
# 		"--n_workers",
# 		type=int,
# 		default=20,
# 		help="Number of parallel processes to run"
# 		)
# 	args = parser.parse_args()

# 	log.info("Parsing input image")
# 	img = glam.getImageType(args.input_file)(args.input_file)
# 	log.info("Building baseline")
# 	updateBaselines(new_image=img,n_workers=args.n_workers)


# if __name__ == "__main__":
# 	main()