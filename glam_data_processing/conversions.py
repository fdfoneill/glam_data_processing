#! /usr/bin/env python3

"""This script converts a shapefile into a mask raster

Given an existing raster to match, the output raster has
identical cell size and extent.
"""

# C:\Python27\ArcGIS10.5\python.exe


## set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)
#log.info(f"{os.path.basename(__file__)} started {datetime.today()}")

import argparse, gdal, glob, os, rasterio, shutil, subprocess, sys
import geopandas as gpd
from pyproj import CRS
from rasterio import features
from rasterio.enums import Resampling
from gdalnumeric import *
#print(sys.executable)


def reprojectRaster(in_raster,model_raster,out_dir,name_override = None):
	"""Given input shapefile, reprojects to match model raster

	***

	Parameters
	----------
	in_shapefile:str
		Path to shapefile on disk
	out_dir:str
		Path to output directory where result will be stored
	name_override:str
		Override default naming convention
	"""
	if name_override:
		out_name = os.path.join(out_dir,name_override)
	else:
		in_base,in_ext = os.path.splitext(os.path.basename(in_raster))
		out_name = os.path.join(out_dir,in_base+"_REPROJ"+in_ext)
		#print(out_name)
	t_ds = gdal.Open(model_raster,0)
	t_wkt = t_ds.GetProjection()
	s_ds = gdal.Open(in_raster,0)
	s_wkt = s_ds.GetProjection()
	t_ds = s_ds = None
	if s_wkt != t_wkt:
		reproj_args = [r"C:\OSGeo4W64\bin\gdalwarp.exe","-t_srs",t_wkt,in_raster,out_name]
		subprocess.call(reproj_args)
		return out_name
	else:
		log.warning("Projections already match, doing nothing.")
		return in_raster


def reprojectShapefile(in_shapefile, model_raster, out_dir, name_override=None) -> str:
	"""Returns two file paths with matching projections

	Reprojects vector to match projection of raster (if
	necessary)

	Parameters
	---------_
	in_shapefile:str
		Path to shapefile that will be reprojected
	model_raster:str
		Path to model raster from which projection
		information will be extracted
	out_dir: str
		Directory on disk where output is to be
		written
	name_override:str
		Override default naming convention

	Returns
	-------
	String path to new reprojected shapefile
	"""
	shapefile_path = in_shapefile # old variable name; standardized to match function parameters
	# get out_path from out_dir
	if name_override:
		out_path = os.path.join(out_dir,name_override)
	else:
		in_base,in_ext = os.path.splitext(os.path.basename(in_shapefile))
		out_path = os.path.join(out_dir,in_base+"_REPROJ"+in_ext)
	# get raster projection as wkt
	with rasterio.open(model_raster,'r') as img:
		raster_wkt = img.profile['crs'].to_wkt()
	# get shapefile projection as wkt
	with open(shapefile_path.replace(".shp",".prj")) as rf:
		shapefile_wkt = rf.read()

	# if it's a match, nothing needs to be done
	if raster_wkt == shapefile_wkt:
		log.warning("CRS already match")
		# get input directory and filename
		in_dir = os.path.dirname(shapefile_path)
		in_name = os.path.splitext(os.path.basename(shapefile_path))[0]
		# list all elements of shapefile
		all_shape_files = glob.glob(os.path.join(in_dir,f"{in_name}.*"))
		# get output directory and filenames
		out_dir = os.path.dirname(out_path)
		out_name = os.path.splitext(os.path.basename(out_path))[0]
		for f in all_shape_files:
			name, ext = os.path.splitext(os.path.basename(f))
			out_f = os.path.join(out_dir,f"{out_name}{ext}")
			with open(f,'rb') as rf:
				with open(out_f,'wb') as wf:
					shutil.copyfileobj(rf,wf)
	else:
		# get CRS objects
		raster_crs = CRS.from_wkt(raster_wkt)
		shapefile_crs = CRS.from_wkt(shapefile_wkt)
		#transformer = Transformer.from_crs(raster_crs,shapefile_crs)

		# convert geometry and crs
		out_shapefile_path = out_path # os.path.join(temp_dir,os.path.basename(shapefile_path))
		data = gpd.read_file(shapefile_path)
		data_proj = data.copy()
		data_proj['geometry'] = data_proj['geometry'].to_crs(raster_crs)
		data_proj.crs = raster_crs

		# save output
		data_proj.to_file(out_shapefile_path)


	return out_shapefile_path


def resampleRaster(in_raster,model_raster,out_dir,name_override=None,method=Resampling.nearest):
	if name_override:
		out_name = os.path.join(out_dir,name_override)
	else:
		in_base,in_ext = os.path.splitext(os.path.basename(in_raster))
		out_name = os.path.join(out_dir,in_base+"_RESAMPLED"+in_ext)
		#print(out_name)
	# get input metadata
	with rasterio.open(in_raster) as ds:
		meta_profile = ds.profile
	width = meta_profile['width']
	height = meta_profile['height']
	# copy input file
	with open(in_raster,'rb') as rf:
		with open(out_name,'wb') as wf:
			shutil.copyfileobj(rf,wf)
	# resample with rasterio
	with rasterio.open(out_name) as dataset:
		# resample data to target shape
		data = dataset.read(
			out_shape = (
				dataset.count,
				height,
				width
			),
			resampling = method
		)
		# scale image transform
		transform = dataset.transform * dataset.transform.scale(
			(dataset.width / data.shape[-1]),
			(dataset.height / data.shape[-2])
		)

	return out_name


def shapefileToRaster(in_shapefile, model_raster, out_dir, name_override=None, zone_field:str = None, dtype = None, *args, **kwargs) -> str:
	"""Burns shapefile into raster image

	***

	Parameters
	----------
	in_shapefile: str
		Path to input shapefile
	model_raster: str
		Path to existing raster dataset. Used for extent,
		pixel size, and other metadata. Output raster
		will be a pixel-for-pixel match of this
		dataset
	out_dir: str
		Directory where output rsater will be written on
		disk
	name_override:str
		Override default naming convention
	zone_field: str
		Field in shapefile to use as raster value. If None,
		flagged pixels will be written as "1" and non-flagged
		pixels will be written as NoData. Default None
	dtype: str
		If set, overrides default int32 dtype with new data type,
		e.g. float32. Default None
	"""
	# correct variable names
	shapefile_path = in_shapefile
	# get out_path
	if name_override:
		out_path = os.path.join(out_dir,name_override)
	else:
		in_base = os.path.splitext(os.path.basename(in_shapefile))[0]
		model_ext = os.path.splitext(model_raster)[1]
		out_path= os.path.join(out_dir,in_base+"_RASTER"+model_ext)
	# read file
	shp = gpd.read_file(shapefile_path)
	with rasterio.open(model_raster,'r') as rst:
		meta = rst.meta.copy()

	# this is where we create a generator of geom, value pairs to use in rasterizing
	if zone_field is not None:
		zone_vals = []
		for i in range(len(shp)):
			zone_vals.append(shp.at[i,zone_field])
		zone_codes = [i for i, val in enumerate(zone_vals)]
		shapes = ((geom,val) for geom, val in zip(shp.geometry,zone_codes))
	else:
		shapes = ((geom,1) for geom in shp.geometry)

	# set data type
	if dtype:
		meta.update(dtype=dtype)
	elif zone_field:
		meta.update(dtype=rasterio.dtypes.get_minimum_dtype(zone_codes))
	else:
		meta.update(dtype="int16")

	try:
		out =  rasterio.open(out_path, 'w+', **meta)
	# merra-2 files have a very high nodata value, beyond the range of int32.
	# This block catches the resulting ValueError and swaps in the minimum
	# allowable data type. Nice of rasterio to have such a function.
	except ValueError:
		meta.update(dtype=rasterio.dtypes.get_minimum_dtype([meta['nodata']]))
		out = rasterio.open(out_path, 'w+', **meta)
		out_arr = out.read(1)
		burned = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
		out.write_band(1, burned)
	out.close()

	return out_path


def clipAndAlignRasters(input_raster,clip_raster,out_dir,name_override=None):
	"""
	Given input and clip raster, produces new raster of input's values,
	but with identical extent to clip.
	"""
	if name_override:
		out_name = os.path.join(out_dir,name_override)
	else:
		in_base,in_ext = os.path.splitext(os.path.basename(input_raster))
		out_name = os.path.join(out_dir,in_base+"_ALIGNED"+in_ext)
	arcpy.env.snapRaster= clip_raster
	arcpy.Clip_management(input_raster,out_raster=out_name,in_template_dataset=clip_raster,maintain_clipping_extent="MAINTAIN_EXTENT")
	return out_name


def rasterToBinary(input_raster,output_dir,name_override=None):
	"""Converts raster with many values to 0 / 1 binary mask"""
	if name_override:
		out_path = os.path.join(output_dir,name_override)
	else:
		in_base,in_ext = os.path.splitext(os.path.basename(input_raster))
		out_path = os.path.join(output_dir,in_base+"_BINARY"+in_ext)

	ds = gdal.Open(input_raster,0)
	band = ds.GetRasterBand(1)
	noData = band.GetNoDataValue()
	srs = ds.GetProjection()
	gt = ds.GetGeoTransform()
	arr = BandReadAsArray(band)
	ds = band = None # close dataset and band
	arr[arr != noData] = 1
	arr[arr == noData] = noData
	rasterYSize, rasterXSize = arr.shape
	driver = gdal.GetDriverByName('GTiff')
	dataset = driver.Create(out_path,rasterXSize,rasterYSize,1,gdal.GDT_Byte,['COMPRESS=DEFLATE'])
	dataset.GetRasterBand(1).WriteArray(arr)
	dataset.GetRasterBand(1).SetNoDataValue(noData)
	dataset.SetGeoTransform(gt)
	dataset.SetProjection(srs)
	dataset.FlushCache() # Write to disk
	del dataset

	return out_path


def cloud_optimize_inPlace(in_file:str,compress="LZW") -> None:
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
	cloudOpArgs = ["gdal_translate",intermediate_file,in_file,'-q','-co', "TILED=YES",'-co',"COPY_SRC_OVERVIEWS=YES",'-co', f"COMPRESS={compress}"]#, "-co", "PREDICTOR=2"]
	subprocess.call(cloudOpArgs)

	## remove intermediate
	os.remove(intermediate_file)


def shapefileConversion(in_shapefile,model_raster,out_dir,name_override=None,clean=True,temp_dir_override=None,binary=True,*args,**kwargs):
	## generate output filename and tempDir
	in_product = os.path.basename(model_raster).split(".")[0]
	in_crop = os.path.basename(in_shapefile).split(".")[0]
	out_base = name_override or "{0}.{1}.tif".format(in_product,in_crop)
	tempDir = temp_dir_override or os.path.join(os.path.dirname(__file__),"temp")
	if not os.path.exists(tempDir):
		os.mkdir(tempDir)

	try:
		log.info("Reprojecting shapefile")
		shpRep = reprojectShapefile(in_shapefile,model_raster,tempDir)
		log.info("Converting to raster")
		rasFinal = shapefileToRaster(shpRep,model_raster,tempDir,*args,**kwargs)
		# log.info("Resampling raster")
		# rasRes = resampleRaster(rasRaw,model_raster,tempDir)
		# log.info("Clipping and aligning extents")
		# if binary:
		# 	rasAligned = clipAndAlignRasters(rasRes,model_raster,tempDir)
		# 	log.info("Converting to binary mask")
		# 	rasFinal = rasterToBinary(rasAligned,out_dir,out_base)
		# else:
		# 	rasFinal = clipAndAlignRasters(rasRes,model_raster,out_dir,out_base)
		log.info("Cloud-optimizing output")
		cloud_optimize_inPlace(rasFinal)
		log.info("Done. Output is at {}".format(rasFinal))
	except:
		log.exception("Something has gone wrong...")
	finally:
		if clean:
			for f in glob.glob(os.path.join(tempDir,"*")):
				os.remove(f)
		else:
			log.warning("Not deleting intermediate files in {}".format(tempDir))


def rasterConversion(in_raster,model_raster,out_dir,name_override=None,clean=True,temp_dir_override=None,binary=True):
	in_product = os.path.basename(model_raster).split(".")[0]
	in_crop = os.path.basename(in_raster).split(".")[0]
	out_base = name_override or "{0}.{1}.tif".format(in_product,in_crop)
	tempDir = temp_dir_override or os.path.join(os.path.dirname(__file__),"temp")
	if not os.path.exists(tempDir):
		os.mkdir(tempDir)
	try:
		log.info("Reprojecting raster")
		rasRep = reprojectRaster(in_raster,model_raster,tempDir)
		log.info("Resampling raster")
		try:
			rasRaw = resampleRaster(rasRep,model_raster,tempDir)
		except:
			log.exception("Failed to resample {}".format(in_raster))
			rasRaw = in_raster

		log.info("Clipping and aligning extents")
		if binary:
			rasAligned = clipAndAlignRasters(rasRaw,model_raster,tempDir)
			log.info("Converting to binary mask")
			rasFinal = rasterToBinary(rasAligned,out_dir,out_base)
		else:
			rasFinal = clipAndAlignRasters(rasRaw,model_raster,out_dir,out_base)
		log.info("Cloud-optimizing output")
		cloud_optimize_inPlace(rasFinal)
		log.info("Done. Output is at {}".format(rasFinal))
	except:
		log.exception("Something has gone wrong...")
	finally:
		if clean:
			for f in glob.glob(os.path.join(tempDir,"*")):
				os.remove(f)
		else:
			log.warning("Not deleting intermediate files in {}".format(tempDir))


def main():
	parser = argparse.ArgumentParser(description="Convert a shapefile to a mask that matches an existing raster dataset")
	parser.add_argument("in_shapefile",
		type=str,
		help="Path to input shapefile on disk")
	parser.add_argument("model_raster",
		type=str,
		help="Path to raster file to be matched")
	parser.add_argument("out_dir",
		type=str,
		help="Path to directory where output will be stored")
	parser.add_argument("-it",
		"--input_type",
		choices=["RASTER","VECTOR"],
		type=str,
		help="Input data type")
	parser.add_argument("-ot",
		"--output_type",
		choices=["MASK","ADMIN"],
		type=str,
		help="Mask or admin (default mask)"
		)
	parser.add_argument("-no",
		"--name_override",
		type=str,
		required=False,
		action='store',
		default=None,
		help="Override the default file naming convention"
		)
	parser.add_argument('-zf',
		'--zone_field',
		type=str,
		required=False,
		default="ID",
		help="If converting shapefile to admin zones, what field name to use for zones"
		)
	parser.add_argument("-k",
		"--keep_intermediate",
		action='store_false',
		help="Do not delete intermediate files")

	args = parser.parse_args()

	if args.output_type == "MASK":
		binary = True
	else:
		binary = False

	if args.input_type == "RASTER":
		rasterConversion(args.in_shapefile,args.model_raster,args.out_dir,args.name_override,args.keep_intermediate,binary=binary)
	else:
		shapefileConversion(args.in_shapefile,args.model_raster,args.out_dir,args.name_override,args.keep_intermediate,binary=binary,args.zone_field)


if __name__ == "__main__":
	main()
