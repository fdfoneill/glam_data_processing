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

	# define handles
	product_handle = rasterio.open(product_path,'r')
	mask_handle = rasterio.open(mask_path,'r')
	admin_handle = rasterio.open(admin_path,'r')

	# get nodata
	product_noDataVal = product_handle.meta['nodata']
	mask_noDataVal = mask_handle.meta['nodata']
	admin_noDataVal = admin_handle.meta['nodata']

	# define data
	product_data = product_handle.read(1,window=targetwindow)
	mask_data = mask_handle.read(1,window=targetwindow)
	admin_data = admin_handle.read(1,window=targetwindow)

	# create empty output dictionary
	out_dict = {}

	# loop over all admin codes present in admin_data
	uniqueadmins = np.unique(admin_data[admin_data != admin_noDataVal]) # exclude nodata value
	for admin_code in uniqueadmins:
		arable_pixels = int((admin_data[(admin_data == admin_code) & (mask_data != product_noDataVal)]).size)
		if arable_pixels == 0:
			continue
		masked = np.array(product_data[(product_data != product_noDataVal) & (mask_data != mask_noDataVal) & (admin_data == admin_code)], dtype='int64')
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
		# weighted mean
		arable_visible_stored = (stored_info["arable_pixels"] * stored_info["percent_arable"] / 100.0)
		arable_visible_this = (this_info["arable_pixels"] * this_info["percent_arable"] / 100.0)
		try:
			stored_weight = arable_visible_stored / (arable_visible_stored + arable_visible_this)
		except ZeroDivisionError:
			stored_weight = 0
		try:
			this_weight = arable_visible_this / (arable_visible_this + arable_visible_stored)
		except ZeroDivisionError:
			this_weight = 0
		## update value
		value = (stored_info['value'] * stored_weight) + (this_info['value'] * this_weight)
		## update arable_pixels
		arable_pixels = stored_info['arable_pixels'] + this_info['arable_pixels']
		## update percent_arable
		percent_arable = (stored_info['percent_arable'] * stored_weight) + (this_info['percent_arable'] * this_weight)
		out_dict[k] = {'value':value,'arable_pixels':arable_pixels,'percent_arable':percent_arable}
	return out_dict



def zonalStats(product_path:str,mask_path:str,admin_path:str,n_cores=1,block_scale_factor=1) -> dict:
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
	"""
	# start timer
	start_time = datetime.now()
	# get metadata
	meta_handle = rasterio.open(product_path,'r')
	meta_profile = meta_handle.profile
	## block size
	if meta_profile['tiled']:
		blocksize = meta_profile['blockxsize'] * block_scale_factor
	else:
		log.warning(f"Input file {product_path} is not tiled!")
		blocksize = 256 * block_scale_factor
	## raster dimensions
	hnum = meta_handle.width
	vnum = meta_handle.height
	## close and delete open rasterio objects
	meta_handle.close()
	del meta_handle
	del meta_profile

	# get windows
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
	log.info(f"Finished processing {product_path} x {mask_path} x {admin_path} in {datetime.now()-start_time}.")

	return final_output