#! /usr/bin/env python

"""

"""

# set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)

# import other required modules
import rasterio#, dask, xarray
import numpy as np
#from dask.distributed import Client
from datetime import datetime
from multiprocessing import Pool
from rasterio.windows import Window


def _mp_worker(args:tuple) -> dict:
	"""A function for use with the multiprocessing
	package, passed to each worker.

	Returns a dictionary of the form:
		{zone_id:{'value':VALUE,'arable_pixels':VALUE,'percent_arable':VALUE},...}

	Parameters
	----------
	args:tuple
		Tuple containing the following (in order):
			targetwindow
			product_path
			mask_path
			admin_path
	"""
	targetwindow, product_path, mask_path, admin_path = args

	if "nomask" in mask_path:
		mask_path = None

	# get product raster info
	product_handle = rasterio.open(product_path,'r')
	product_noDataVal = product_handle.meta['nodata']
	product_data = product_handle.read(1,window=targetwindow)
	product_handle.close()

	# get mask raster info
	if mask_path is not None:
		mask_handle = rasterio.open(mask_path,'r')
		mask_noDataVal = mask_handle.meta['nodata']
		mask_data = mask_handle.read(1,window=targetwindow)
		mask_handle.close()
	else:
		mask_data = np.full(product_data.shape, 1)

	# get admin raster info
	admin_handle = rasterio.open(admin_path,'r')
	admin_noDataVal = admin_handle.meta['nodata']	
	admin_data = admin_handle.read(1,window=targetwindow)	
	admin_handle.close()	

	# create empty output dictionary
	out_dict = {}

	# loop over all admin codes present in admin_data
	uniqueadmins = np.unique(admin_data[admin_data != admin_noDataVal]) # exclude nodata value
	for admin_code in uniqueadmins:
		arable_pixels = int((admin_data[(admin_data == admin_code) & (mask_data == 1)]).size)
		if arable_pixels == 0:
			continue
		masked = np.array(product_data[(product_data != product_noDataVal) & (mask_data == 1) & (admin_data == admin_code)], dtype='int64')
		percent_arable = (float(masked.size) / float(arable_pixels)) * 100
		value = (masked.mean() if (masked.size > 0) else 0)
		out_dict[admin_code] = {"value":value,"arable_pixels":arable_pixels,"percent_arable":percent_arable}

	return out_dict


def _update(stored_dict,this_dict) -> dict:
	"""Updates stats dictionary with values from a new window result

	Parameters
	----------
	stored_dict:dict
		Dictionary to be updated with new data
	this_dict:dict
		New data with which to update stored_dict
	"""
	out_dict = stored_dict
	for k in this_dict.keys():
		this_info = this_dict[k]
		try:
			stored_info = stored_dict[k]
		except KeyError: # if stored_dict has no info for zone k (new zone in this window), set it equal to the info from this_dict
			out_dict[k] = this_info
			continue
		# calculate number of visible arable pixels for both dicts by multiplying arable_pixels with percent_arable
		arable_visible_stored = (stored_info["arable_pixels"] * stored_info["percent_arable"] / 100.0)
		arable_visible_this = (this_info["arable_pixels"] * this_info["percent_arable"] / 100.0)
		try:
			# weight of stored_dict value is the ratio of its visible arable pixels to the total number of visible arable pixels
			stored_weight = arable_visible_stored / (arable_visible_stored + arable_visible_this)
		except ZeroDivisionError:
			# if no visible pixels at all, weight everything at 0
			stored_weight = 0
		try:
			# weight of this_dict value is the ratio of its visible arable pixels to the total number of visible arable pixels
			this_weight = arable_visible_this / (arable_visible_this + arable_visible_stored)
		except ZeroDivisionError:
			# if the total visible arable pixels are 0, everything gets weight 0
			this_weight = 0
		## weighted mean value
		value = (stored_info['value'] * stored_weight) + (this_info['value'] * this_weight)
		## sum of arable pixels
		arable_pixels = stored_info['arable_pixels'] + this_info['arable_pixels']
		## directly recalculate total percent arable from sum of arable_visible divided by arable_pixels
		percent_arable = ((arable_visible_stored + arable_visible_this) / arable_pixels) * 100
		#percent_arable = (stored_info['percent_arable'] * stored_weight) + (this_info['percent_arable'] * this_weight)
		out_dict[k] = {'value':value,'arable_pixels':arable_pixels,'percent_arable':percent_arable}
	return out_dict


def get_validBounds(raster_path:str, mask_or_admin: str = "MASK") -> tuple:
	"""A function to find the boundaries of a mask raster file's non-nodata pixels

	TOO SLOW FOR BIG FILES

	Returns a tuple of (top,left,bottom,right) bounding box indices

	Parameters
	----------
	raster_path:str
		Full path to mask or admin file on disk
	mask_or_admin:str
		Choices are "MASK" or "ADMIN"; determines whether the function
		searches for '1' values (mask) or non-nodata values (admin)
	"""
	log.warning("get_validBounds() takes too much memory for large raster files")
	with rasterio.open(raster_path,'r') as raster_handle:
		width = raster_handle.width
		height = raster_handle.height
		raster_nodataValue = raster_handle.profile['nodata']
		raster_data = raster_handle.read()

	raster_data = raster_data.reshape(height,width) # flatten third dimension
	if mask_or_admin == "MASK":
		valid_indices = np.where(raster_data == 1)
	elif mask_or_admin == "ADMIN":
		valid_indices = np.where(raster_data != raster_nodataValue)
	valid_index_list = list(zip(valid_indices[0], valid_indices[1]))
	ul = valid_index_list[0]
	lr = valid_index_list[-1]

	return (*ul,*lr)


def zonalStats(product_path:str, mask_path:str, admin_path:str, n_cores: int = 1, block_scale_factor: int = 8, default_block_size: int = 256) -> dict:
	"""A function for calculating zonal statistics on a raster image

	Returns a dictionary of the form:
		{zone_id:{'value':VALUE,'arable_pixels':VALUE,'percent_arable':VALUE},...}

	Parameters
	----------
	product_path:str
		Path to product dataset on disk
	mask_path:str
		Path to crop mask dataset on disk
	admin_path:str
		Path to admin dataset on disk
	n_cores:int
		Number of cores to use for parallel processing. Default is 1
	block_scale_factor:int
		Relative size of processing windows compared to product_path native block
		size. Default is 8, calculated to be optimal for all n_cores (1-50) on 
		GEOG cluster node 18
	default_block_size:int
		If product_path is not tiled, this argument is used as the block size. In
		that case, windows will be of size (default_block size * block_scale_factor)
		on each side.
	"""
	# start timer
	start_time = datetime.now()
	# coerce numeric arguments to correct type
	n_cores = int(n_cores)
	block_scale_factor = int(block_scale_factor)
	# get metadata
	with rasterio.open(product_path,'r') as meta_handle:
		meta_profile = meta_handle.profile
		## block size
		if meta_profile['tiled']:
			blocksize =meta_profile['blockxsize'] * block_scale_factor
		else:
			log.warning(f"Input file {product_path} is not tiled!")
			blocksize = 256 * block_scale_factor
		## raster dimensions
		hnum = meta_handle.width
		vnum = meta_handle.height

	# get windows
	windows = []
	# log.info(type(hnum))
	# log.info(type(blocksize))
	for hstart in range(0, hnum, blocksize):
		for vstart in range(0, vnum, blocksize):
			hwin = blocksize
			vwin = blocksize
			if ((hstart + blocksize) > hnum):
				hwin = (hnum % blocksize)
			if ((vstart + blocksize) > vnum):
				vwin = (vnum % blocksize)
			targetwindow = Window(hstart, vstart, hwin, vwin)
			windows += [targetwindow]

	# generate parallel args
	parallel_args = [(w, product_path, mask_path, admin_path) for w in windows]

	# note progress
	checkpoint_1_time = datetime.now()
	log.debug(f"Finished preparing in {checkpoint_1_time-start_time}.\nStarting parallel processing on {n_cores} core(s).")

	# do parallel
	final_output = {}
	p = Pool(processes=n_cores)
	for window_output in p.map(_mp_worker, parallel_args):
		_update(final_output, window_output)
	p.close()
	p.join()

	# note final time
	log.debug(f"Finished parallel processing in {datetime.now()-checkpoint_1_time}.")
	log.debug(f"Finished processing {product_path} x {mask_path} x {admin_path} in {datetime.now()-start_time}.")

	return final_output