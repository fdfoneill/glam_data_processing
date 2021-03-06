#! /usr/bin/env python3

"""

"""

# set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)

import shutil, subprocess
from .exceptions import BadInputError
from rasterio.windows import Window
import numpy as np

# constants
NDVI_PRODUCTS = ["MOD09Q1","MOD13Q1","MYD09Q1","MYD13Q1","VNP09H1","MOD09Q1N","MOD13Q4N","MOD09CMG","VNP09CMG"]
ANCILLARY_PRODUCTS = ["chirps","chirps-prelim","swi","merra-2"]
RASTER_DIR = os.path.join("/gpfs","data1","cmongp2","GLAM","rasters")


def getMetadata(image_path:str) -> dict:
    """Parses metadata from a productdataset filename

    ***

    Parameters
    ----------
    image_path: str
        String path to GLAM productdataset file on disk

    Returns
    -------
    Dictonary with the following key/value pairs:
        path: equal to image_path argument
        product: name of input file product
        category: whether input is NDVI or ancillary
        date_format: '%Y.%j' or '%Y-%m-%d'
        date_object: date as datetime object
        date: date string in format YYYY-MM-DD
        year: year string
        doy: 3-digit 0-padded doy string
    """
    metadata = {}
    metadata['path'] = image_path
    basename = os.path.basename(image_path)
    # get product name and parse date
    name_parts = basename.split(".")
    product_raw = name_parts[0]
    metadata['product'] = product_raw
    ## ndvi products use YYYY.DOY date format
    if product_raw in NDVI_PRODUCTS:
        metadata['category'] = "NDVI"
        metadata['date_format'] = "%Y.%j"
        year, doy = name_parts[1:3]
        date_live = datetime.strptime(f"{year}.{doy}","%Y.%j")
        date = date_live.strftime("%Y-%m-%d")
    ## ancillary products use YYYY-MM-DD date format
    elif product_raw in ANCILLARY_PRODUCTS:
        metadata['category'] = "ancillary"
        metadata['date_format'] = "%Y-%m-%d"
        date_live = datetime.strptime(name_parts[1],"%Y-%m-%d")
        year = date_live.strftime("%Y")
        doy = date_live.strftime("%j")
        date = date_live.strftime("%Y-%m-%d")
    else:
        raise BadInputError(f"Failed to parse product from '{basename}'")
    # write date variables to metadata dict
    metadata['date_obj'] = date_live
    metadata['date'] = date
    metadata['year'] = year
    metadata['doy'] = doy
    # fix merra-2 product name
    if product_raw == "merra-2":
        sub_product = name_parts[2]
        full_product = "merra-2-"+sub_product
        metadata['product'] = full_product

    # return result dict
    return metadata


def cloud_optimize_inPlace(in_file:str) -> None:
	"""Takes path to input and output file location. Reads tif at input location and writes cloud-optimized geotiff of same data to output location."""
	## add overviews to file
	cloudOpArgs = ["gdaladdo",in_file,"-quiet"]
	subprocess.call(cloudOpArgs)

	## copy file
	intermediate_file = in_file.replace(".tif",".TEMP.tif")
	with open(intermediate_file,'wb') as a:
		with open(in_file,'rb') as b:
			shutil.copyfileobj(b,a)

	## add tiling to file
	cloudOpArgs = ["gdal_translate",intermediate_file,in_file,'-q','-co', "TILED=YES",'-co',"COPY_SRC_OVERVIEWS=YES",'-co', "COMPRESS=LZW", "-co", "PREDICTOR=2"]
	if getMetadata(in_file)['product'] in NDVI_PRODUCTS:
		cloudOpArgs.append("-co")
		cloudOpArgs.append("BIGTIFF=YES")
	subprocess.call(cloudOpArgs)

	## remove intermediate
	os.remove(intermediate_file)


def getWindows(width, height, blocksize) -> list:
	"""Given width, height, and block size, returns a list of rasterio.windows.Window objects covering entire image"""
	hnum, vnum = width, height
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
	return windows


def getValidRange(dtype:str) -> tuple:
	"""Returns minimum and maximum valid values in given data type (e.g. int32)"""
	try:
		if (dtype == "byte") or ("int" in dtype):
			try:
				return (np.iinfo(dtype).min, np.iinfo(dtype).max)
			except:
				raise ValueError
		else:
			raise ValueError
	except ValueError:
		raise ValueError(f"Data type '{dtype}' not recognized by glam_data_processing.stats.getValidRange()")
