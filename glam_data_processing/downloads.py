#! /usr/bin/env python

"""

"""

# set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)

import boto3, collections, ftplib, glob, gdal, gzip, json, math, octvi, re, requests, shutil, subprocess, sys, urllib
from botocore.exceptions import ClientError
boto3.set_stream_logger('botocore', level='INFO')
from datetime import datetime
from ftplib import FTP
from gdalnumeric import *
gdal.UseExceptions()
import numpy as np
from osgeo import osr
from urllib.error import URLError
from urllib.request import urlopen, Request, URLError, HTTPError

from glam_data_processing.exceptions import BadInputError, NoCredentialsError, UnavailableError

## getting credentials

def readCredentialsFile() -> None:
	"""Reads merra / swi usernames and passwords from credentials file into environment variables"""
	#log.info(__file__)
	credDir = os.path.dirname(os.path.dirname(__file__))
	credFile = os.path.join(credDir,"glam_keys.json")
	#log.info(credFile)
	try:
		with open(credFile,'r') as f:
			keys = json.loads(f.read())
		log.debug(f"Found credentials file: {credFile}")
	except FileNotFoundError:
		raise NoCredentialsError("Credentials file ('glam_keys.json') not found.")
	for k in ["merrausername","merrapassword","swiusername","swipassword"]:
		try:
			log.debug(f"Adding variable to environment: {k}")
			os.environ[k] = keys[k]
		except KeyError:
			log.debug(f"Variable not found in credentials file")
			continue


def pullFromSource(product:str,date:str,output_directory:str,file_name_override:str = None) -> tuple:
	"""A function that pulls available imagery from its source repository

	Parameters
	----------
	product: str
		Name of desired product; e.g. "MOD09Q1"
	date: str
		Date of desired image in format "YYYY-MM-DD" or "YYYY.DOY"
	output_directory: str
		Path to output directory where file will be downloaded
	file_name_override: str
		Default None; if set, overrides the default output file naming
		convention of "PRODUCT.DATE.tif", "PRODUCT.YYYY.DOY.tif", or
		"PRODUCT.DATE.COLLECTION.tif", depending on product. Currently
		only implemented for NDVI products (see octvi.supported_products)

	Returns
	-------
	String path to output location of downloaded file on disk
	"""

	# find repository credentials
	try:
		readCredentialsFile()
	except NoCredentialsError:
		log.warning("No credentials file found. Reading directly from environment variables.")
	credentials = {}
	try:
		credentials['merraUsername'] = os.environ['merrausername']
		credentials['merraPassword'] = os.environ['merrapassword']
		credentials['swiUsername'] = os.environ['swiusername']
		credentials['swiPassword'] = os.environ['swipassword']
	except KeyError:
		log.error("At least one data access credential is not set. Use 'glamconfigure' on command line to set archive credentials.")
		return ()


	# helper functions


	def project_to_sinusoidal_inPlace(in_file:str) -> int:
			"""
			This function, given a path to a file, projects it to sinusoidal in place

			...

			Parameters
			----------
			in_file:str
				string path to input file; file name must match "{product}.{date}*.tif"

			Return values
			-------------
			0: function executed successfully
			1: projection failed
			2: gdal failed to read file
			"""
			product = os.path.basename(in_file).split(".")[0]
			# Merra-2 and CHIRPS come as un-projected arrays, and must be defined to geographic before projecting to sinusoidal
			if len(gdal.Open(in_file,0).GetProjection()) == 0:
				srs = osr.SpatialReference() # create SpatialReference object
				srs.ImportFromEPSG(4326) # standard geographic coordinate system EPSG number
				ds = None
				try:
					ds = gdal.Open(in_file,gdal.GA_Update) # open file in update mode
				except RuntimeError: # thrown if file path is bad
					log.error(f"--projection failed: {in_file} does not exist")
				if ds:
					res = ds.SetProjection(srs.ExportToWkt()) # define projection to geographic
					if res != 0:
						log.error("--projection failed: {}".format(str(res)))
						return 1
					ds = None
				else:
					log.error("--could not open with GDAL")
					return 2
			# reproject to sinusoidal
			## copy to intermediate file
			intermediate_file = in_file.replace(".tif",".TEMP.tif")
			with open(intermediate_file,'wb') as a:
				with open(in_file,'rb') as b:
					shutil.copyfileobj(b,a)
			#print(in_file,intermediate_file)
			sinus = 'PROJCS["Sinusoidal",GEOGCS["GCS_Undefined",DATUM["Undefined",SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Sinusoidal"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],UNIT["Meter",1.0]]' # MODIS sinusoidal projection
			if product:# == "swi":
				intF= gdal.Open(intermediate_file,0)
				in_srs = intF.GetProjection()
				intF = None
				gdal.Warp(in_file,intermediate_file,srcSRS = in_srs,dstSRS = sinus) #
			else:
				gdal.Warp(in_file,intermediate_file,dstSRS = sinus) # reproject back to original file name, this time in MODIS sinusoidal
			os.remove(intermediate_file) # delete intermediate file
			# CHECK EXTENTS AGAINST MODIS
			needsClipped = False
			ds = gdal.Open(in_file,0)
			gt = ds.GetGeoTransform()
			cell = gt[1]
			x = ds.RasterXSize
			y = ds.RasterYSize
			north = gt[3]
			west = gt[0]
			east = west+(cell*x)
			south = north-(cell*y)
			maxNorth= 9962342# 9972315.0495 * 0.999
			maxWest= -22735470# -22758229.000 * 0.999
			maxSouth = -9143189# -9152341.5816 * 0.999
			maxEast= 20958445# 20979424.893 * 0.999
			if north > maxNorth:
				north = maxNorth
				needsClipped = True
			if west < maxWest:
				west = maxWest
				needsClipped = True
			if south < maxSouth:
				south = maxSouth
				needsClipped = True
			if east > maxEast:
				east = maxEast
				needsClipped = True
			if needsClipped:
				# clip with GDAL_translate
				clipFile = in_file.replace(".tif",".CLIPPING.tif")
				with open(clipFile,'wb') as a:
					with open(in_file,'rb') as b:
						shutil.copyfileobj(b,a)
				clip_args = ["gdal_translate", "-q", "-projwin",str(west),str(north),str(east),str(south),clipFile,in_file]
				subprocess.call(clip_args)
				os.remove(clipFile)
			return 0


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
		subprocess.call(cloudOpArgs)

		## remove intermediate
		os.remove(intermediate_file)


	# individual download functions


	def downloadMerra2(date:str, out_dir:str, credentials:dict, variable="ALL", *args, **kwargs) -> tuple:
		"""
		Given date of merra product, downloads file to output directory
		Returns tuple of file paths or empty list if download failed
		Downloaded files are COGs in sinusoidal projection

		Parameters
		----------
		date:str
		out_dir:str
		credentials:dict
		variable:str
			Options = [MIN, MEAN, MAX, ALL]
		"""

		# handle variable options
		minimum = maximum = mean = False
		if variable == "ALL":
			minimum = True
			maximum = True
			mean = True
		elif variable == "MIN":
			minimum = True
		elif variable == "MAX":
			maximum = True
		elif variable == "MEAN":
			mean = True
		else:
			raise BadInputError(f"Parameter 'variable' expected one of MIN, MEAN, MAX, not '{variable}'")



		## define how to mosaic list of file paths
		def mosaic_merra2(path_list:list,metric:str) -> str:
			"""
			Takes a list of file paths, and mosaics results to a file ('merra-2.{date}.{metric}.tif') in output directory
			Returns file path to mosaic
			"""
			def create_array_from_dataset(path):
				"""Converts data set to NumPy array"""
				sds = gdal.Open(path,0)
				band = sds.GetRasterBand(1)
				outArray=BandReadAsArray(band)
				del sds
				return outArray
			def save_array_to_file(in_array,out_path):
				"""Takes numPy array, out file path, and model file (for size, geo_transform, and projection) to create geoTiff output"""
				driver = gdal.GetDriverByName('GTiff')
				dataset = driver.Create(out_path,576,361,1,gdal.GDT_Float32,['COMPRESS=LZW'])
				dataset.GetRasterBand(1).WriteArray(in_array)
				dataset.SetGeoTransform((-180.3125, 0.625, 0.0, 90.25, 0.0, -0.5))
				dataset.GetRasterBand(1).SetNoDataValue(1E15)
				dataset.FlushCache() # Write to disk
				del dataset
				return 0

			out = os.path.join(out_dir,f"merra-2.{date}.tif")

			metric_arrays = []
			for f in path_list:
				metric_arrays.append(create_array_from_dataset(f))
			metric_cube = np.dstack(metric_arrays)
			if metric == "min":
				oa = np.amin(metric_cube,axis=2)
				on = out.replace(".tif",".min.tif")
			elif metric == "max":
				oa = np.amax(metric_cube,axis=2)
				on = out.replace(".tif",".max.tif")
			elif metric == "mean":
				oa = np.mean(metric_cube,axis=2)
				on = out.replace(".tif",".mean.tif")
			else:
				w=f"WARNING: {k} does not match known stat (min, max, mean)\nSource: Merra-2, {out}"
				log.warning(w)
			save_array_to_file(oa,on)
			return on

		## generate list of urls (we have to mosaic merra)
		m2Urls = []
		for i in range(5): # we are collecting the requested date along with 4 previous days
			mDate = datetime.strptime(date,"%Y-%m-%d") - timedelta(days=i)
			mYear = mDate.strftime("%Y")
			mMonth = mDate.strftime("%m").zfill(2)
			mDay = mDate.strftime("%d").zfill(2)
			pageUrl = f"https://goldsmr4.gesdisc.eosdis.nasa.gov/data/MERRA2/M2SDNXSLV.5.12.4/{mYear}/{mMonth}/"
			try:
				pageObject = urlopen(pageUrl)
				pageText = str(pageObject.read())
				ex = re.compile(f'MERRA2\S*{mYear}{mMonth}{mDay}.nc4') # regular expression that matches file name of desired date file
				mFileName = re.search(ex,pageText).group() # matches desired file name from web page
				mUrl = pageUrl + mFileName
				m2Urls.append(mUrl)
			except HTTPError: # if any file in the range is missing, don't generate the mosaic at all
				log.warning(f"No Merra-2 file exists for {date}")
				return ()

		## dictionary of empty lists of metric-specific file paths, waiting to be filled
		merraFiles = {}
		if minimum:
			merraFiles['min']=[]
		if maximum:
			merraFiles['max']=[]
		if mean:
			merraFiles['mean']=[]

		## loop over list of urls
		log.debug(m2Urls)
		for url in m2Urls:
			urlDate = url.split(".")[-2]
			## use requests module to download MERRA-2 file (.nc4)
			with requests.Session() as session:
				session.auth = (credentials['merraUsername'], credentials['merraPassword'])
				r1 = session.request('get',url)
				r = session.get(r1.url,auth=(credentials['merraUsername'], credentials['merraPassword']))
			outNc4 = os.path.join(out_dir,f"merra-2.{urlDate}.NETCDF.TEMP.nc") # NetCDF file path
			# write output .nc4 file
			with open(outNc4,"wb") as fd: # write data in chunks
				for chunk in r.iter_content(chunk_size = 1024*1024):
					fd.write(chunk)

			##CHECKSUM
			observedSize = int(os.stat(outNc4).st_size) # size of downloaded file in bytes
			expectedSize = int(r.headers['Content-Length']) # size of promised file in bytes, extracted from server-delivered headers

			## checksum failure; return empty tuple
			if observedSize != expectedSize: # checksum failure
				w=f"WARNING:\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
				log.warning(w)
				return () # no files for you today, but we'll try again tomorrow!


			# as of now it's a .nc4 file, we need TIFFs of just the 3 subdatasets we're interested in (T2MMEAN, T2MIN, T2MAX)

			## extract subdataset names
			dataset= gdal.Open(outNc4,0)

			if minimum:
				sdMin = dataset.GetSubDatasets()[3][0] # full name of T2MMIN subdataset
				minOut = os.path.join(out_dir,f"M2_MIN.{urlDate}.TEMP.tif")
				subprocess.call(["gdal_translate","-q",sdMin,minOut]) # calling the command line to produce the tiff
				merraFiles['min'].append(minOut)

			if maximum:
				sdMax = dataset.GetSubDatasets()[1][0] # full name of T2MMAX subdataset
				maxOut = os.path.join(out_dir,f"M2_MAX.{urlDate}.TEMP.tif")
				subprocess.call(["gdal_translate","-q",sdMax,maxOut]) # calling the command line to produce the tiff
				merraFiles['max'].append(maxOut)

			if mean:
				sdMean = dataset.GetSubDatasets()[2][0] # full name of T2MMEAN subdataset
				meanOut = os.path.join(out_dir,f"M2_MEAN.{urlDate}.TEMP.tif")
				subprocess.call(["gdal_translate","-q",sdMean,meanOut]) # calling the command line to produce the tiff
				merraFiles['mean'].append(meanOut)


			## delete netCDF file and GDAL dataset object
			del dataset
			os.remove(outNc4)



		# merraFiles now stores a list of files for each metric type
		# for each metric, we mosaic all the files by that metric
		# e.g. minimum of the "mins", maximum of all "maxes", mean of all "means"

		## loop over metric in ("min","mean","max")
		merraOut = []
		log.debug(merraFiles)
		for metric in merraFiles.keys():

			## get list of files to be mosaicked
			mosaic_fileList = merraFiles[metric]

			## mosaic into out product
			mosaicPath = mosaic_merra2(mosaic_fileList, metric)

			## delete individual files
			for f in mosaic_fileList:
				os.remove(f)

			## project mosaic to sinusoidal
			project_to_sinusoidal_inPlace(mosaicPath)

			## cloud-optimize mosaic
			cloud_optimize_inPlace(mosaicPath)

			## add mosaic to list
			merraOut.append(mosaicPath)

		## return tuple of mosaic paths
		return tuple(merraOut)


	def downloadChirps(date:str, out_dir:str, *args, **kwargs) -> tuple:
		"""
		Given date of chirps product, downloads file to directory
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""

		try:
			## define file locations
			file_unzipped = os.path.join(out_dir,f"chirps.{date}.tif") # output location for final file
			file_zipped=file_unzipped+".gz" # initial location for zipped version of file

			## get url to be downloaded
			cDate = datetime.strptime(date,"%Y-%m-%d")
			cYear = cDate.strftime("%Y")
			cMonth = cDate.strftime("%m").zfill(2)
			cDay = str(int(np.ceil(int(cDate.strftime("%d"))/10)))
			# url = f"ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif.gz"
			url = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif"
			print(url)

			## download file at url
			# try:
			# 	with open(file_zipped,"w+b") as fz:
			# 		fh = urlopen(Request(url))
			# 		shutil.copyfileobj(fh,fz)
			# except URLError:
			# 	log.warning(f"No Chirps file exists for {date}")
			# 	os.remove(file_zipped)
			# 	return ()
			#
			# ## checksum
			# observedSize = int(os.stat(file_zipped).st_size) # size of downloaded file (bytes)
			# expectedSize = int(urlopen(Request(url)).info().get("Content-length")) # size anticipated from header (bytes)
			#
			# ## checksum fails, log warning and return empty list
			# if observedSize != expectedSize:
			# 	w=f"WARNING:\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
			# 	log.warning(w)
			# 	os.remove(file_zipped)
			# 	return () # no files for you today :(
			#
			# ## use gzip to unzip file to final location
			# tf = file_unzipped.replace(".tif",".UNMASKED.tif")
			# with gzip.open(file_zipped) as fz:
			# 	with open(tf,"w+b") as fu:
			# 		shutil.copyfileobj(fz,fu)
			# os.remove(file_zipped) # delete zipped version

			## new downloading method (https)

			with requests.Session() as session:
				r1 = session.request('get',url)
				r = session.get(r1.url)
			if r.status_code != 200:
				log.warning(f"Url {url} not found")
				return ()
			# write output .nc4 file
			with open(file_zipped,"wb") as fd: # write data in chunks
				for chunk in r.iter_content(chunk_size = 1024*1024):
					fd.write(chunk)

			##CHECKSUM
			observedSize = int(os.stat(file_zipped).st_size) # size of downloaded file in bytes
			expectedSize = int(r.headers['Content-Length']) # size of promised file in bytes, extracted from server-delivered headers

			## checksum failure; return empty tuple
			if observedSize != expectedSize: # checksum failure
				w=f"WARNING:\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
				log.warning(w)
				return () # no files for you today, but we'll try again tomorrow!

			## use gzip to unzip file to final location
			tf = file_unzipped.replace(".tif",".UNMASKED.tif")
			with gzip.open(file_zipped) as fz:
				with open(tf,"w+b") as fu:
					shutil.copyfileobj(fz,fu)
			os.remove(file_zipped) # delete zipped version

			## apply nodata mask to file
			chirps_noData_args = ["gdal_translate","-q","-a_nodata", "-9999",tf,file_unzipped] # apply NoData mask
			subprocess.call(chirps_noData_args)
			os.remove(tf) # delete unmasked file

			## project file to sinusoidal
			prj = project_to_sinusoidal_inPlace(file_unzipped)
			if prj != 0:
				log.warning(f"Failed to project {file_unzipped}")
				return ()

			## cloud-optimize file
			cloud_optimize_inPlace(file_unzipped)

			# ## remove corresponding preliminary product, if necessary
			# correspondingPrelimFile = os.path.join(out_dir,f"chirps-prelim.{date}.tif")
			# if os.path.exists(correspondingPrelimFile):
			# 	os.remove(correspondingPrelimFile)

			## return file path string in tuple
			return tuple([file_unzipped])

		except Exception as e: # catch unhandled error; log warning message; return failure in form of empty tuple
			log.exception(f"Unhandled error downloading chirps for {date}")
			return ()


	def downloadChirpsPrelim(date:str, out_dir:str, *args, **kwargs) -> tuple:
		"""
		Given date of chirps preliminary data product, downloads file to directory
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""
		try:
			file_out = os.path.join(out_dir,f"chirps-prelim.{date}.tif")
			tf = file_out.replace(".tif",".UNMASKED.tif")

			## get url to be downloaded
			cDate = datetime.strptime(date,"%Y-%m-%d")
			cYear = cDate.strftime("%Y")
			cMonth = cDate.strftime("%m").zfill(2)
			cDay = str(int(np.ceil(int(cDate.strftime("%d"))/10)))
			# url = f"ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/prelim/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif"
			url = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif"

			# ## download file at url
			# try:
			# 	with open(tf,"w+b") as fd:
			# 		fs = urlopen(Request(url))
			# 		shutil.copyfileobj(fs,fd)
			# except URLError:
			# 	log.warning(f"No Chirps file exists for {date}")
			# 	os.remove(tf)
			# 	return ()

			## new downloading method (https)

			with requests.Session() as session:
				r1 = session.request('get',url)
				r = session.get(r1.url)
			if r.status_code != 200:
				log.warning(f"Url {url} not found")
				return ()
			# write output tif file
			with open(tf,"wb") as fd: # write data in chunks
				for chunk in r.iter_content(chunk_size = 1024*1024):
					fd.write(chunk)

			## checksum
			observedSize = int(os.stat(tf).st_size) # size of downloaded file (bytes)
			expectedSize = int(urlopen(Request(url)).info().get("Content-length")) # size anticipated from header (bytes)

			## checksum fails, log warning and return empty list
			if observedSize != expectedSize:
				w=f"WARNING:\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
				log.warning(w)
				os.remove(tf)
				return ()

			## apply nodata mask to file
			chirps_noData_args = ["gdal_translate","-q","-a_nodata", "-9999",tf,file_out] # apply NoData mask
			subprocess.call(chirps_noData_args)
			os.remove(tf) # delete unmasked file

			## project file to sinusoidal
			prj = project_to_sinusoidal_inPlace(file_out)
			if prj != 0:
				log.warning(f"Failed to project {file_out}")
				return ()

			## cloud-optimize file
			cloud_optimize_inPlace(file_out)

			## return tuple of file path
			return tuple([file_out])

		except Exception as e: # catch unhandled error; log warning message; return failure in form of empty tuple
			log.exception(f"Unhandled error downloading chirps-prelim for {date}")
			return ()


	def downloadSwi(date:str, out_dir:str, credentials:dict, *args, **kwargs) -> tuple:
		"""
		Given date of swi product, downloads file to directory if possible
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""

		out = os.path.join(out_dir,f"swi.{date}.tif")
		dateObj = datetime.strptime(date,"%Y-%m-%d") # convert string date to datetime object
		year = dateObj.strftime("%Y")
		month = dateObj.strftime("%m".zfill(2))
		day = dateObj.strftime("%d".zfill(2))

		## download file
		url = f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/Daily_SWI_12.5km_Global_V3/{year}/{month}/{day}/SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1/c_gls_SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1.nc"
		file_nc = out.replace("tif","nc") # temporary NetCDF file; later to be converted to tiff

		## use requests module to download MERRA-2 file (.nc4)
		with requests.Session() as session:
			session.auth = (credentials['swiUsername'], credentials['swiPassword'])
			r1 = session.request('get',url)
			r = session.get(r1.url,auth=(credentials['swiUsername'], credentials['swiPassword']))
			headers = r.headers
		# write output .nc file
		with open(file_nc,"wb") as fd: # write data in chunks
			for chunk in r.iter_content(chunk_size = 1024*1024):
				fd.write(chunk)

		## checksum
		observedSize = int(os.stat(file_nc).st_size) # size of downloaded file (bytes)
		expectedSize = int(headers['Content-Length']) # size anticipated from header (bytes)

		if int(observedSize) != int(expectedSize):
			w=f"\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
			log.warning(w)
			os.remove(file_nc)
			return ()

		## extract desired band as GeoTiff
		dataset= gdal.Open(file_nc,0)
		# search all subdatasets for 10-day swi
		for sd in dataset.GetSubDatasets():
			if sd[0].split(":")[-1] == "SWI_010":
				subdataset=sd[0]
		del dataset
		# translate subdataset to its own tiff and delete full netCDF
		swiArgs = ["gdal_translate","-q",subdataset,out] # arguments for gdal to transform subdataset into independent Tiff
		subprocess.call(swiArgs) # calling the command line to produce the tiff
		os.remove(file_nc)

		## project file to sinusoidal
		prj = project_to_sinusoidal_inPlace(out)
		if prj != 0:
			log.warning(f"Failed to project {out}")
			return ()

		## cloud-optimize file
		cloud_optimize_inPlace(out)

		## return tuple of file path string
		return tuple([out])


	def downloadMod09q1(date:str, out_dir:str, file_name_override:str = None, *args, **kwargs) -> tuple:
		"""
		Given date of MOD09Q1 product, downloads file to directory if possible
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""
		product = "MOD09Q1"
		jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
		if file_name_override is not None:
			outPath = os.path.join(out_dir,file_name_override)
		else:
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

		return tuple([octvi.globalVi(product,date,outPath,overwrite=True)])


	def downloadMyd09q1(date:str, out_dir:str, file_name_override:str = None, *args, **kwargs) -> tuple:
		"""
		Given date of MYD09Q1 product, downloads file to directory if possible
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""
		product = "MYD09Q1"
		jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
		if file_name_override is not None:
			outPath = os.path.join(out_dir,file_name_override)
		else:
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

		return tuple([octvi.globalVi(product,date,outPath,overwrite=True)])


	def downloadMod13q1(date:str, out_dir:str, file_name_override:str = None, *args, **kwargs) -> tuple:
		"""
		Given date of MOD13Q1 product, downloads file to directory if possible
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""
		product = "MOD13Q1"
		jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
		if file_name_override is not None:
			outPath = os.path.join(out_dir,file_name_override)
		else:
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")
		return tuple([octvi.globalVi(product,date,outPath,overwrite=True)])


	def downloadMyd13q1 (date:str, out_dir:str, file_name_override:str = None, *args, **kwargs) -> tuple:
		"""
		Given date of MYD13Q1 product, downloads file to directory if possible
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""
		product = "MYD13Q1"
		jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
		if file_name_override is not None:
			outPath = os.path.join(out_dir,file_name_override)
		else:
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

		return tuple([octvi.globalVi(product,date,outPath,overwrite=True)])


	def downloadMod13q4n (date:str, out_dir:str, file_name_override:str = None, *args, **kwargs) -> tuple:
		"""
		Given date of MYD13Q1 product, downloads file to directory if possible
		Returns tuple containing file path or empty list if download failed
		Downloaded files are COGs in sinusoidal projection
		"""
		product = "MOD13Q4N"
		jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
		if file_name_override is not None:
			outPath = os.path.join(out_dir,file_name_override)
		else:
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

		return tuple([octvi.globalVi(product,date,outPath,overwrite=True)])


	# special merra-2 wrapper functions


	def downloadMerra2Min(date:str, out_dir:str, credentials: dict, *args, **kwargs) -> tuple:
		"""Wrapper function around downloadMerra2()"""
		return downloadMerra2(date, out_dir, credentials, variable= "MIN", *args, **kwargs)


	def downloadMerra2Max(date:str, out_dir:str, credentials: dict, *args, **kwargs) -> tuple:
		"""Wrapper function around downloadMerra2()"""
		return downloadMerra2(date, out_dir, credentials, variable= "MAX", *args, **kwargs)


	def downloadMerra2Mean(date:str, out_dir:str, credentials: dict, *args, **kwargs) -> tuple:
		"""Wrapper function around downloadMerra2()"""
		return downloadMerra2(date, out_dir, credentials, variable= "MEAN", *args, **kwargs)


	# actual calling of the functions

	actions = {
		"swi":downloadSwi,
		"chirps":downloadChirps,
		"chirps-prelim":downloadChirpsPrelim,
		"merra-2":downloadMerra2,
		"merra-2-min":downloadMerra2Min,
		"merra-2-max":downloadMerra2Max,
		"merra-2-mean":downloadMerra2Mean,
		"MOD09Q1":downloadMod09q1,
		"MYD09Q1":downloadMyd09q1,
		"MOD13Q1":downloadMod13q1,
		"MYD13Q1":downloadMyd13q1,
		"MOD13Q4N":downloadMod13q4n
		}

	# format date to YYYY-MM-DD
	try:
		date = datetime.strptime(date,"%Y-%m-%d").strftime("%Y-%m-%d")
	except:
		try:
			date = datetime.strptime(date,"%Y.%j").strftime("%Y-%m-%d")
		except:
			raise BadInputError("Date must be of format YYYY-MM-DD or YYYY.DOY")

	return actions[product](date=date, out_dir=output_directory, file_name_override = file_name_override, credentials=credentials)


def pullFromS3(product:str,date:str,out_dir:str,collection=0) -> tuple:
	"""
	Pulls desired product x date combination from S3 bucket and downloads to out_dir
	Downloaded files are COGs in sinusoidal projection
	Returns tuple of downloaded product path strings, or empty tuple on failure

	...

	Parameters
	----------
	product:str
		string representation of desired product type ('merra-2','chirps','swi')
	date:str
		string representation in format YYYY.MM.DD or YYYY.DOY of desired product date
	out_dir:str
		path to output directory, where the cloud-optimized geoTiff will be stored
	"""

	## set up boto3 logger, client, and bucket name
	try:
		s3_client = boto3.client('s3',
			aws_access_key_id=os.environ['AWS_accessKeyId'],
			aws_secret_access_key=os.environ['AWS_secretAccessKey']
			)
	except KeyError:
		raise NoCredentialsError("Amazon Web Services (AWS) credentials not found. Use 'glamconfigure' or 'aws configure' on the command line.")
	s3_bucket = 'glam-tc-data'

	## define output list to be coerced to tuple and returned
	results = []

	## format date to YYYY-MM-DD
	try:
		date = datetime.strptime(date,"%Y-%m-%d").strftime("%Y-%m-%d")
	except:
		try:
			date = datetime.strptime(date,"%Y.%j").strftime("%Y-%m-%d")
		except:
			raise BadInputError("Date must be of format YYYY-MM-DD or YYYY.DOY")

	## if the requested product is merra-2, check whether the user specified a collection
	if product == 'merra-2' and collection == 0:
		for metric in ("mean","min","max"):
			s3_key = f"rasters/{product}.{date}.{metric}.tif"
			outFile = os.path.join(out_dir,f"{product}.{date}.{metric}.tif")
			results.append(outFile)
			try:
				s3_client.download_file(s3_bucket,s3_key,outFile)
			except ClientError:
				log.error("File not available on S3")
				return ()
			except Exception:
				log.exception("File download from S3 failed")
				return ()
	elif product == 'merra-2':
		s3_key = f"rasters/{product}.{date}.{collection}.tif"
		outFile = os.path.join(out_dir,f"{product}.{date}.{collection}.tif")
		results.append(outFile)
		try:
			s3_client.download_file(s3_bucket,s3_key,outFile)
		except ClientError:
			log.error("File not available on S3")
			return ()
		except Exception:
			log.exception("File download from S3 failed")
			return ()

	elif product in ["swi","chirps","chirps-prelim"]:
		s3_key = f"rasters/{product}.{date}.tif"
		outFile = os.path.join(out_dir,f"{product}.{date}.tif")
		results.append(outFile)
		try:
			s3_client.download_file(s3_bucket,s3_key,outFile)
		except ClientError:
			log.error("File not available on S3")
			return ()
		except Exception:
			log.exception("File download from S3 failed")
			return ()
	else: # it's an NDVI product
		year, doy = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j").split(".")
		s3_key = f"rasters/{product}.{year}.{doy}.tif"
		outFile = os.path.join(out_dir,f"{product}.{year}.{doy}.tif")
		results.append(outFile)
		try:
			s3_client.download_file(s3_bucket,s3_key,outFile)
		except ClientError:
			log.error("File not available on S3")
			return ()
		except Exception:
			log.exception("File download from S3 failed")
			return ()

	## return tuple of file paths
	return tuple(results)


def isAvailable(product:str, date:str) -> bool:
	"""Returns whether imagery for given product and date is available for download from source"""

	# parse arguments
	try:
		datetime.date(datetime.strptime(date,"%Y-%m-%d"))
	except:
		try:
			date = datetime.strptime(date,"%Y.%j").strftime("%Y-%m-%d")
		except:
			raise BadInputError(f"Failed to parse input '{date}' as a date. Please use format YYYY-MM-DD or YYYY.DOY")

	if product in ["merra-2", "merra-2-min", "merra-2-max", "merra-2-mean"]:
		product = "merra-2"
		allGood = True
		for i in range(5): # we are collecting the requested date along with 4 previous days
			mDate = datetime.strptime(date,"%Y-%m-%d") - timedelta(days=i)
			mYear = mDate.strftime("%Y")
			mMonth = mDate.strftime("%m").zfill(2)
			mDay = mDate.strftime("%d").zfill(2)
			pageUrl = f"https://goldsmr4.gesdisc.eosdis.nasa.gov/data/MERRA2/M2SDNXSLV.5.12.4/{mYear}/{mMonth}/"
			try:
				pageObject = urlopen(pageUrl)
				pageText = str(pageObject.read())
				ex = re.compile(f'MERRA2\S*{mYear}{mMonth}{mDay}.nc4') # regular expression that matches file name of desired date file
				mFileName = re.search(ex,pageText).group() # matches desired file name from web page
			except HTTPError: # if any file in the range is missing, don't generate the mosaic at all
				return False
			except AttributeError:
				log.warning(f"Failed to find Merra-2 URL for {mDate.strftime('%Y-%m-%d')}. We seem to have caught the merra-2 team in the middle of uploading their data... check {pageUrl} to be sure.")
				return False
		return True

	elif product == "chirps":
		## get url to be downloaded
			cDate = datetime.strptime(date,"%Y-%m-%d")
			cYear = cDate.strftime("%Y")
			cMonth = cDate.strftime("%m").zfill(2)
			cDay = str(int(np.ceil(int(cDate.strftime("%d"))/10)))
			url = f"ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif.gz"
			## try to open url
			try:
				fh = urlopen(Request(url))
				return True
			except URLError:
				return False

	elif product == "chirps-prelim":
		## get url to be downloaded
		cDate = datetime.strptime(date,"%Y-%m-%d")
		cYear = cDate.strftime("%Y")
		cMonth = cDate.strftime("%m").zfill(2)
		cDay = str(int(np.ceil(int(cDate.strftime("%d"))/10)))
		url = f"ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/prelim/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif"
		## try to open url
		try:
			fh = urlopen(Request(url))
			return True
		except URLError:
			return False

	elif product == "swi":
		dateObj = datetime.strptime(date,"%Y-%m-%d") # convert string date to datetime object
		year = dateObj.strftime("%Y")
		month = dateObj.strftime("%m".zfill(2))
		day = dateObj.strftime("%d".zfill(2))
		url = f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/Daily_SWI_12.5km_Global_V3/{year}/{month}/{day}/SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1/c_gls_SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1.nc"
		with requests.Session() as session:
			session.auth = (self.swiUsername, self.swiPassword)
			request = session.request('get',url)
			if request.status_code == 200:
				if request.headers['Content-Type'] == 'application/octet-stream':
					return True
			else:
				return False

	elif product in octvi.supported_products:
		if len(octvi.url.getDates(product,date)) > 0:
			return True
		else:
			return False

	else:
		raise BadInputError(f"Product '{product}' not recognized")


def listAvailable(product:str, start_date:str, format_doy = False) -> list:
	"""A function that lists availabe-to-download imagery dates

	Parameters
	----------
	product:str
		String name of product; e.g. MOD09Q1, merra-2-min, etc.
	start_date:str
		Date from which to begin search. Should be the latest
		product date already ingested. Can be formatted as
		YYYY-MM-DD or YYYY.DOY
	format_doy:bool
		Default false. If true, returns dates as YYYY.DOY rather
		than default YYYY-MM-DD

	Returns
	-------
	List of string dates in format YYYY-MM-DD or YYYY.DOY, depending
	on format_doy argument. Each date has available imagery to download.
	"""

	# parse arguments
	try:
		latest = datetime.date(datetime.strptime(start_date,"%Y-%m-%d"))
	except:
		try:
			latest = datetime.date(datetime.strptime(start_date,"%Y.%j"))
		except:
			raise BadInputError(f"Failed to parse input '{start_date}' as a date. Please use format YYYY-MM-DD or YYYY.DOY")
	today = datetime.date(datetime.today())
	raw_dates = []
	filtered_dates = []

	if product in ["merra-2", "merra-2-min", "merra-2-max", "merra-2-mean"]:
		product = "merra-2"
		# get all possible dates
		while latest < today:
			latest = latest + timedelta(days=1)
			log.debug(f"Found missing file in valid date range: merra-2 for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "chirps":
		# get all possible dates
		while latest < today:
			if int(latest.strftime("%d")) > 12:
				latest = latest+timedelta(days=15) # push the date into the next month, but not past the 11th day of the next month
				latest = datetime.date(datetime.strptime(latest.strftime("%Y-%m")+"-01","%Y-%m-%d")) # once we're in next month, slam the day back down to 01
			else:
				latest = latest+timedelta(days=10) # 01 becomes 11, 11 becomes 21
			log.debug(f"Found missing file in valid date range: chirps for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "chirps-prelim":
		# get all possible dates
		while latest < today:
			if int(latest.strftime("%d")) > 12:
				latest = latest+timedelta(days=15) # push the date into the next month, but not past the 11th day of the next month
				latest = datetime.date(datetime.strptime(latest.strftime("%Y-%m")+"-01","%Y-%m-%d")) # once we're in next month, slam the day back down to 01
			else:
				latest = latest+timedelta(days=10) # 01 becomes 11, 11 becomes 21
			log.debug(f"Found missing file in valid date range: chirps-prelim for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "swi":
		# get all possible dates
		while latest < today:
			latest = latest + timedelta(days=5)
			log.debug(f"Found missing file in valid date range: swi for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "MOD09Q1":
		# get all possible dates
		if latest is None:
			latest = datetime.date(datetime.strptime("2000.049","%Y.%j"))
		while latest < today:
			oldYear = latest.strftime("%Y")
			latest = latest + timedelta(days = 8)
			if latest.strftime("%Y") != oldYear:
				latest = latest.replace(day=1)
			log.debug(f"Found missing file in valid date range: MOD09Q1 for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "MYD09Q1":
		# get all possible dates
		if latest is None:
			latest = datetime.date(datetime.strptime("2002.185","%Y.%j"))
		while latest < today:
			oldYear = latest.strftime("%Y")
			latest = latest + timedelta(days = 8)
			if latest.strftime("%Y") != oldYear:
				latest = latest.replace(day=1)
			log.debug(f"Found missing file in valid date range: MYD09Q1 for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "MOD13Q1":
		# get all possible dates
		if latest is None:
			latest = datetime.date(datetime.strptime("2000.049","%Y.%j"))
		while latest < today:
			oldYear = latest.strftime("%Y")
			latest = latest + timedelta(days = 16)
			if latest.strftime("%Y") != oldYear:
				latest = latest.replace(day=1)
			log.debug(f"Found missing file in valid date range: MOD13Q1 for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "MYD13Q1":
		# get all possible dates
		if latest is None:
			latest = datetime.date(datetime.strptime("2002.185","%Y.%j"))
		while latest < today:
			oldYear = latest.strftime("%Y")
			latest = latest + timedelta(days = 16)
			if latest.strftime("%Y") != oldYear:
				latest = latest.replace(day=9)
			log.debug(f"Found missing file in valid date range: MYD13Q1 for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	elif product == "MOD13Q4N":
		# get all possible dates
		if latest is None:
			latest = datetime.date(datetime.strptime("2002.185","%Y.%j"))
		while latest < today:
			oldYear = latest.strftime("%Y")
			latest = latest + timedelta(days = 1)
			if latest.strftime("%Y") != oldYear:
				latest = latest.replace(day=1)
			log.debug(f"Found missing file in valid date range: MOD13Q4N for {latest.strftime('%Y-%m-%d')}")
			raw_dates.append(latest.strftime("%Y-%m-%d"))

	else:
		raise BadInputError(f"Product '{product}' not recognized")

	# filter products
	for rd in raw_dates:
		if isAvailable(product,rd):
			filtered_dates.append(rd)

	# convert to DOY format if requested
	if format_doy:
		temp_dates = filtered_dates
		filtered_dates = []
		for td in temp_dates:
			filtered_dates.append(datetime.strptime(td,"%Y-%m-%d").strftime("%Y.%j"))

	return filtered_dates
