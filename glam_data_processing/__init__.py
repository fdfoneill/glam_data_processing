#! /usr/bin/env python

##############################################################################
# Author: F. Dan O'Neill
# Date: 10/04/2019
# Script title: glam_data_processing.py
##############################################################################

"""glam_data_processing

This module facilitates handling of the various types of imagery date used
in the NASA Harvest GLAM system. These data types include NDVI imagery from
MODIS, CHIRPS rainfall, Copernicus Soil-Water Index, and MERRA-2 temperature.

Imagery can be pulled from source archives or the GLAM AWS S3 bucket using 
the Downloader class. Downloaded files can then be used to create Image objects,
which allow for ingestion to the system and the calculation of regional
statistics.

***

Classes
-------
BadInputError
NoCredentialsError
UploadFailureError
UnavailableError
RecordNotFoundError
ToDoList
	Collects all missing files that are potentially available for each data
	stream. Can be pared down to only those that are actually available with
	the filterUnavailable() method. Callable.
Downloader
	Allows for downloading of all data streams. pullFromSource method pulls
	from original archives, while pullFromS3 downloads from the GLAM AWS
	S3 bucket. isAvailable method checks image availability.
Image
	Prototype for AncillaryImage and ModisImage. Allows getting and setting
	status in database, ingesting image, and uploading regional statistics.
	Create by passing path to image file on disk to constructor.
AncillaryImage
	This Image subclass should be used for all non-NDVI files.
ModisImage
	This Image subclass should be used for all NDVI files.

Functions
---------
readCredentialsFile:None
	Attempts to read glam_keys.json and write to environment variables. If no
	credentials file exists in the expected location (two directory levels
	above __init__.py), raises NoCredentialsError.
getImageType:Image
	Given path to an image file on disk, returns the appropriate Image
	subclass. File must be well-named, i.e. 'PRODUCT.etc.TIF'
purge:bool
	Removes all trace of a given image file from database. Intended for
	use with chirps-prelim data, which is no longer useful once final chirps
	data becomes available. Requires authorization key. Use with extreme
	caution.

Command-Line Scripts
--------------------
glaminfo
	Prints a summary of the package's classes, functions, and scripts, along
	with the current version number.
glamconfigure
	Prompts user input to input credentials for password-protected data archives,
	as well as the GLAM system database. These credentials are stored in
	'glam_keys.json', two directories above __init__.py. AWS credentials should
	instead be set through the 'awsconfigure' script of awscli.
glamupdatedata
	Performs an update on all data streams. First finds missing files and checks
	for availability. For each available file, downloads, ingests, and calculates
	statistics. Downloaded files are then deleted.
"""

from ._version import __version__

## set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)
#log.info(f"{os.path.basename(__file__)} started {datetime.today()}")

## import modules
import sys, glob, gdal, boto3, hashlib, json, math, gzip, octvi, shutil, requests, ftplib, re, subprocess, collections, urllib
from botocore.exceptions import ClientError
boto3.set_stream_logger('botocore', level='INFO')
from gdalnumeric import *
gdal.UseExceptions()
from osgeo import osr
from urllib.error import URLError
from urllib.request import urlopen, Request, URLError, HTTPError
from ftplib import FTP
import numpy as np
import pandas as pd
import terracotta as tc 
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

ancillary_products = ["chirps","chirps-prelim","swi","merra-2"]

admins_gaul = ["gaul1"]
admins_brazil = ["BR_Mesoregion","BR_Microregion","BR_Municipality","BR_State"]
admins = admins_gaul + admins_brazil

# make crops_brazil_info
crops_brazil_info = {}

crops_cropmonitor = ["maize","rice","soybean","springwheat","winterwheat","cropland"]
crops_brazil = ['2S-DFZSafraZ2013_2014', '2S-GOZSafraZ2013_2014', '2S-MAZSafraZ2013_2014', '2S-MGZSafraZ2013_2014', '2S-MSZSafraZ2013_2014', '2S-MTZSafraZ2013_2014', '2S-PIZSafraZ2013_2014', '2S-PRZSafraZ2012_2013', '2S-SPZSafraZ2013_2014', '2S-TOZSafraZ2013_2014', 'CV-DFZSafraZ2017_2018', 'CV-GOZSafraZ2014_2015', 'CV-MATOPIBAZSafraZ2013_2014', 'CV-MGZSafraZ2013_2014', 'CV-MSZSafraZ2014_2015', 'CV-MTZSafraZ2014_2015', 'CV-PRZSafraZ2013_2014', 'CV-ROZSafraZ2013_2014', 'CV-RSZSafraZ2011_2012', 'CV-SCZSafraZ2013_2014', 'CV-SPZSafraZ2014_2015']#list(crops_brazil_info.keys())
crops = crops_cropmonitor + crops_brazil + ["nomask"]

## decorators

def log_io(func):
	def wrapped(*args,**kwargs):
		result = func(*args,**kwargs)
		log.debug(f"{func.__name__} called with arguments " + " ".join((str(a) for a in args)) + f" with result {str(result)}")
		return result
	return wrapped

## custom error classes

class BadInputError(Exception):   
	def __init__(self, data):
		self.data = data
	def __str__(self):
		return repr(self.data)

class UploadFailureError(Exception):   
	def __init__(self, data):	
		self.data = data
	def __str__(self):
		return repr(self.data)

class UnavailableError(Exception):
	def __init__(self,data):
		self.data=data
	def __str__(self):
		return repr(self.data)

class RecordNotFoundError(Exception):
	def __init__(self,data):
		self.data=data
	def __str__(self):
		return repr(self.data)

class NoCredentialsError(Exception):
	def __init__(self,data):
		self.data=data
	def __str__(self):
		return repr(self.data)

## getting credentials

def readCredentialsFile() -> None:
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
	for k in ["merrausername","merrapassword","swiusername","swipassword","glam_mysql_user","glam_mysql_pass"]:
		try:
			log.debug(f"Adding variable to environment: {k}")
			os.environ[k] = keys[k]
		except KeyError:
			log.debug(f"Variable not found in credentials file")
			continue

try:
	readCredentialsFile()
except NoCredentialsError:
	log.warning("No credentials file found. Reading directly from environment variables instead.")

## checking for statscode

statscodeDir = os.path.join(os.path.dirname(__file__),"statscode")
if not os.path.exists(statscodeDir):
	log.warning("'statscode' directory not found. This folder should be in the same directory as __init__.py, and should contain all the crop mask and admin region rasters. Image objects cannot be instantialized without it.")

## other class definitions

# instance creates and stores list of pending-download files for each type (merra,chirps,swi). Note that the attribute is named merra and not merra-2
class ToDoList:
	"""
	A class to generate and store lists of files that need to be checked for availability and, if available, downloaded

	An instance of this class can, in addition to its attributes and methods, be treated as an iterator. It will yield
	a series of tuples, of the form ("PRODUCT","DATE"), for the dates in all of its product attributes (see below). If
	the instance is called (i.e. ToDoList()), each of these tuples will be printed.

	...

	Attributes
	----------
	engine: sqlalchemy engine object
		this database engine is connected to the glam system database
	metadata: sqlalchemy metadata object
		stores the metadata
	product_status: sqlalchemy table object
		a table recording the extent to which the image has been processed into the glam system
	chirps:list
		a list of string dates (%Y-%m-%d), representing potentially available chirps files
	chirps_prelim:list
		a list of string dates (%Y-%m-%d), representing potentially available chirps preliminary data files
	merra:list
		a list of string dates (%Y-%m-%d), representing potentially available merra-2 files
	mod09q1:list
		a list of string dates ("%Y-%m-%d"), representing potentially available MOD09Q1 files
	myd09q1:list
		a list of string dates ("%Y-%m-%d"), representing potentially available MYD09Q1 files
	mod13q1:list
		a list of string dates ("%Y-%m-%d"), representing potentially available MOD13Q1 files
	myd13q1:list
		a list of string dates ("%Y-%m-%d"), representing potentially available MYD13Q1 files
	swi:list
		a list of string dates (%Y-%m-%d), representing potentially available swi files

	Methods
	-------
	refresh:None
		essentially re-initializes instance, capturing any changes to the database (i.e. files that have been added since the last update)
	filterUnavailable:None
		removes images that are not yet available for download
	"""

	# mysql credentials
	try:
		mysql_user = os.environ['glam_mysql_user']
		mysql_pass = os.environ['glam_mysql_pass']
		mysql_db = 'modis_dev'

		engine = db.create_engine(f'mysql+pymysql://{mysql_user}:{mysql_pass}@glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com/{mysql_db}')
		metadata = db.MetaData()
		product_status = db.Table('product_status',metadata,autoload=True,autoload_with=engine)

	except KeyError:
		log.warning("Database credentials not found. ToDoList objects cannot be instantialized. Use 'glamconfigure' on command line to set archive credentials.")

	

	def __init__(self):
		
		self.refresh()

	def __repr__(self):
		interjection = "" if self.filtered else "not "
		return f"<Instance of ToDoList, refreshed {self.timestamp}, {interjection}filtered>"

	def __iter__(self):
		for d in self.chirps:
			yield ("chirps",d)
		for d in self.chirps_prelim:
			yield ("chirps-prelim",d)
		for d in self.merra:
			yield ("merra-2",d)
		for d in self.swi:
			yield ("swi",d)
		for d in self.mod09q1:
			yield ("MOD09Q1",d)
		for d in self.myd09q1:
			yield ("MYD09Q1",d)
		for d in self.mod13q1:
			yield ("MOD13Q1",d)
		for d in self.myd13q1:
			yield ("MYD13Q1",d)

	def __call__(self):
		for f in self:
			print(f)

	def refresh(self) -> None:

		def getDbMissing(prod:str) -> list:
			"""Return list of string dates of files in database where 'product'=={prod} and 'downloaded'==False"""
			with self.engine.begin() as connection:
				r = connection.execute(f"SELECT date FROM product_status WHERE product='{prod}' AND completed = 0;").fetchall()
			return [x[0].strftime("%Y-%m-%d") for x in r]

		def getLatestDate(prod:str) -> datetime.date:
			with self.engine.begin() as connection:
				return connection.execute(f"SELECT MAX(date) FROM product_status WHERE product='{prod}';").fetchone()[0] # gets datetime.date object

		def updateDatabase(product:str,unprocessed_date_string:str) -> None:
			with self.engine.begin() as connection:
				connection.execute(f"INSERT INTO product_status (product, date, downloaded, processed, completed) VALUES ('{product}', '{unprocessed_date_string}', 0, 0, 0);")

		def getAllMerra2() -> list:
			"""Return list of string dates of all missing merra-2 files"""

			def getDbMerra2() -> list:
				"""Return list of string dates of merra-2 files in database where 'downloaded'==False"""
				return getDbMissing('merra-2')

			def getChronoMerra2() -> list:
				"""Return list of string dates of merra-2 files between last database entry and current time"""
				latest = getLatestDate('merra-2')
				today = datetime.date(datetime.today())
				cm2 = [] # output list of merra-2 files
				while latest < today:
					latest = latest + timedelta(days=1)
					log.debug(f"Found missing file in valid date range: merra-2 for {latest.strftime('%Y-%m-%d')}")
					cm2.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('merra-2',latest.strftime("%Y-%m-%d"))
				return cm2

			return getDbMerra2() + getChronoMerra2()

		def getAllChirps() -> list:
			"""Return list of string dates of all missing chirps files"""

			def getDbChirps() -> list:
				"""Return list of string dates of chirps files in database where 'downloaded'==False"""
				return getDbMissing('chirps')

			def getChronoChirps() -> list:
				"""Return list of string dates of chirps files between last database entry and current time"""
				latest = getLatestDate('chirps')
				today = datetime.date(datetime.today())
				cc = []
				while latest < today:
					if int(latest.strftime("%d")) > 12:
						latest = latest+timedelta(days=15) # push the date into the next month, but not past the 11th day of the next month
						latest = datetime.date(datetime.strptime(latest.strftime("%Y-%m")+"-01","%Y-%m-%d")) # once we're in next month, slam the day back down to 01
					else:
						latest = latest+timedelta(days=10) # 01 becomes 11, 11 becomes 21
					log.debug(f"Found missing file in valid date range: chirps for {latest.strftime('%Y-%m-%d')}")
					cc.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('chirps',latest.strftime("%Y-%m-%d"))
				return cc

			return getDbChirps() + getChronoChirps()

		def getAllChirpsPrelim() -> list:
			"""Return list of string dates of all missing preliminary chirps files"""
			def getDbChirpsPrelim() -> list:
				"""Return list of string dates of chirps-prelim files in database where 'downloaded'==False"""
				return getDbMissing('chirps-prelim')

			def getChronoChirpsPrelim() -> list:
				"""Return list of string dates of chirps-prelim files between last database entry and current time"""
				latest = getLatestDate('chirps-prelim')
				today = datetime.date(datetime.today())
				cc = []
				while latest < today:
					if int(latest.strftime("%d")) > 12:
						latest = latest+timedelta(days=15) # push the date into the next month, but not past the 11th day of the next month
						latest = datetime.date(datetime.strptime(latest.strftime("%Y-%m")+"-01","%Y-%m-%d")) # once we're in next month, slam the day back down to 01
					else:
						latest = latest+timedelta(days=10) # 01 becomes 11, 11 becomes 21
					log.debug(f"Found missing file in valid date range: chirps-prelim for {latest.strftime('%Y-%m-%d')}")
					cc.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('chirps-prelim',latest.strftime("%Y-%m-%d"))
				return cc

			return getDbChirpsPrelim() + getChronoChirpsPrelim()

		def getAllSwi() -> list:
			"""Return list of string dates of all missing swi files"""

			def getDbSwi() -> list:
				"""Return list of string dates of swi files in database where 'downloaded'==False"""
				return getDbMissing('swi')

			def getChronoSwi() -> list:
				"""Return list of string dates of swi files between last database entry and current time"""
				latest = getLatestDate('swi')
				today = datetime.date(datetime.today())
				cs = []
				while latest < today:
					latest = latest + timedelta(days=5)
					log.debug(f"Found missing file in valid date range: swi for {latest.strftime('%Y-%m-%d')}")
					cs.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('swi',latest.strftime("%Y-%m-%d"))
				return cs

			return getDbSwi() + getChronoSwi()

		def getAllMod09q1() -> list:
			"""Return list of string dates of all missing MOD09Q1 files"""

			def getChronoMod09q1() -> list:
				"""Return list of string dates of MOD09Q1 files between last database entry and current time"""
				latest = getLatestDate("MOD09Q1")
				if latest is None:
					latest = datetime.date(datetime.strptime("2000.049","%Y.%j"))
				today = datetime.date(datetime.today())
				cm = []
				while latest < today:
					oldYear = latest.strftime("%Y")
					latest = latest + timedelta(days = 8)
					if latest.strftime("%Y") != oldYear:
						latest = latest.replace(day=1)
					log.debug(f"Found missing file in valid date range: MOD09Q1 for {latest.strftime('%Y-%m-%d')}")
					cm.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('MOD09Q1',latest.strftime("%Y-%m-%d"))
				return cm

			return getDbMissing("MOD09Q1") + getChronoMod09q1()

		def getAllMyd09q1() -> list:
			"""Return list of string dates of all missing MYD09Q1 files"""

			def getChronoMyd09q1() -> list:
				"""Return list of string dates for MYD09Q1 files between last database entry and current time"""
				latest = getLatestDate("MYD09Q1")
				if latest is None:
					latest = datetime.date(datetime.strptime("2002.185","%Y.%j"))
				today = datetime.date(datetime.today())
				cm = []
				while latest < today:
					oldYear = latest.strftime("%Y")
					latest = latest + timedelta(days = 8)
					if latest.strftime("%Y") != oldYear:
						latest = latest.replace(day=1)
					log.debug(f"Found missing file in valid date range: MYD09Q1 for {latest.strftime('%Y-%m-%d')}")
					cm.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('MYD09Q1',latest.strftime("%Y-%m-%d"))
				return cm

			return getDbMissing("MYD09Q1") + getChronoMyd09q1()

		def getAllMod13q1() -> list:
			"""Return list of string dates of all missing MOD13Q1 files"""
			
			def getChronoMod13q1() -> list:
				"""Return list of string dates for MOD13Q1 files between last database entry and current time"""
				latest = getLatestDate("MOD13Q1")
				if latest is None:
					latest = datetime.date(datetime.strptime("2000.049","%Y.%j"))
				today = datetime.date(datetime.today())
				cm = []
				while latest < today:
					oldYear = latest.strftime("%Y")
					latest = latest + timedelta(days = 16)
					if latest.strftime("%Y") != oldYear:
						latest = latest.replace(day=1)
					log.debug(f"Found missing file in valid date range: MOD13Q1 for {latest.strftime('%Y-%m-%d')}")
					cm.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('MOD13Q1',latest.strftime("%Y-%m-%d"))
				return cm

			return getDbMissing("MOD13Q1") + getChronoMod13q1()

		def getAllMyd13q1() -> list:
			"""Return list of string dates of all missing MYD13Q1 files"""
			
			def getChronoMyd13q1() -> list:
				"""Return list of string dates for MYD09Q1 files between last database entry and current time"""
				latest = getLatestDate("MYD13Q1")
				if latest is None:
					latest = datetime.date(datetime.strptime("2002.185","%Y.%j"))
				today = datetime.date(datetime.today())
				cm = []
				while latest < today:
					oldYear = latest.strftime("%Y")
					latest = latest + timedelta(days = 16)
					if latest.strftime("%Y") != oldYear:
						latest = latest.replace(day=9)
					log.debug(f"Found missing file in valid date range: MYD13Q1 for {latest.strftime('%Y-%m-%d')}")
					cm.append(latest.strftime("%Y-%m-%d"))
					updateDatabase('MYD13Q1',latest.strftime("%Y-%m-%d"))
				return cm

			return getDbMissing("MYD13Q1") + getChronoMyd13q1()

		with self.engine.begin() as connection:
			connection.execute("UPDATE product_status SET completed = 1 WHERE processed = 1 AND statGen = 1;")

		self.merra = getAllMerra2()
		self.chirps = getAllChirps()
		self.chirps_prelim = getAllChirpsPrelim()
		self.swi = getAllSwi()
		self.mod09q1 = getAllMod09q1()
		self.myd09q1 = getAllMyd09q1()
		self.mod13q1 = getAllMod13q1()
		self.myd13q1 = getAllMyd13q1()
		self.timestamp = datetime.now()
		self.filtered = False

	def filterUnavailable(self) -> None:
		filterMachine = Downloader()
		self.chirps = [f for f in self.chirps if filterMachine.isAvailable('chirps',f)]
		self.chirps_prelim = [f for f in self.chirps_prelim if filterMachine.isAvailable('chirps-prelim',f)]
		self.merra = [f for f in self.merra if filterMachine.isAvailable('merra-2',f)]
		self.swi = [f for f in self.swi if filterMachine.isAvailable('swi',f)]
		self.mod09q1 = [f for f in self.mod09q1 if filterMachine.isAvailable("MOD09Q1",f)]
		self.myd09q1 = [f for f in self.myd09q1 if filterMachine.isAvailable("MYD09Q1",f)]
		self.mod13q1 = [f for f in self.mod13q1 if filterMachine.isAvailable("MOD13Q1",f)]
		self.myd13q1 = [f for f in self.myd13q1 if filterMachine.isAvailable("MYD13Q1",f)]
		self.filtered = True

# only two methods, but they do it all. Pull files from either the S3 bucket or the source archives
class Downloader:
	"""
	A class that can be used to download any of the data types used in the GLAM system

	...

	Attributes
	----------

	merraUsername:str
		username for the merra-2 data archive
	merraPassword:str
		password for the merra-2 data archive
	swiUsername:str
		username for the GLAM SWI ftp server
	swiPassword:
		password for the GLAM SWI ftp server

	Methods
	-------
	isAvailable(product:str,date:str) -> bool:
		checks whether the given product/date combination is available to download
	pullFromSource(product:str,date:str,out_dir:str) -> tuple:
		locates and downloads requested product from source if possible, returns tuple of file paths or empty tuple on failure
	pullFromS3(product:str,date:str,out_dir:str) -> bool:
		locates and downloads requested product from aws S3 bucket if possible, returns False if not
	getAllS3({product:str},{date:str}) -> list:
		gets list of all ingested imagery that matches product and/or date provided
	listMissing(directory:str) -> list:
		given directory with some imagery in it (all of the same product), returns list of missing imagery available on S3
	"""
	# data archive credentials
	credentials = False
	try:
		merraUsername = os.environ['merrausername']
		merraPassword = os.environ['merrapassword']
		swiUsername = os.environ['swiusername']
		swiPassword = os.environ['swipassword']
		credentials = True
	except KeyError:
		log.warning("Data archive credentials not set. The following functionality will be unavailable:\n\tDownloader.isAvailable()\n\tDownloader.pullFromSource()\nUse 'glamconfigure' on command line to set archive credentials.")

	# mysql credentials
	noCred = None
	try:
		mysql_user = os.environ['glam_mysql_user']
		mysql_pass = os.environ['glam_mysql_pass']
		mysql_db = 'modis_dev'

		engine = db.create_engine(f'mysql+pymysql://{mysql_user}:{mysql_pass}@glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com/{mysql_db}')
		metadata = db.MetaData()
		masks = db.Table('masks', metadata, autoload=True, autoload_with=engine)
		regions = db.Table('regions', metadata, autoload=True, autoload_with=engine)
		products = db.Table('products', metadata, autoload=True, autoload_with=engine)
		stats = db.Table('stats', metadata, autoload=True, autoload_with=engine)
		product_status = db.Table('product_status',metadata,autoload=True,autoload_with=engine)
	except KeyError:
		noCred = True


	def __repr__(self):
		return f"<Instance of Downloader, address {id(self)}>"

	def isAvailable(self,product:str,date:str) -> bool:
		"""
		Checks whether product file is available for given date
		NOTE: Most of this code is duplicated in the relevant download{product} functions; there may be a more DRY way to write it
		"""
		
		def checkMerra(date:str) -> bool:
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
			return True

		def checkChirps(date:str) -> bool:
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

		def checkChirpsPrelim(date:str) -> bool:
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

		def checkSwi(date:str) -> bool:

			dateObj = datetime.strptime(date,"%Y-%m-%d") # convert string date to datetime object
			year = dateObj.strftime("%Y")
			month = dateObj.strftime("%m".zfill(2))
			day = dateObj.strftime("%d".zfill(2))
			url = f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/Daily_SWI_12.5km_Global_V3/{year}/{month}/{day}/SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1/c_gls_SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1.nc"
			#print(url)

			with requests.Session() as session:
				session.auth = (self.swiUsername, self.swiPassword)
				request = session.request('get',url)
				if request.status_code == 200:
					if request.headers['Content-Type'] == 'application/octet-stream':
						return True
				else:
					return False

			#r1 = requests.get(url)
			#request = requests.get(url,auth=(self.swiUsername,self.swiPassword))
			#if request.status_code == 200:
			#	return True
			#else:
			#	return False

			#try:
				### log into copernicus
				#log.debug("log in to Copernicus...")
				#con = FTP("ftp.copernicus.vgt.vito.be",self.swiUsername,self.swiPassword)
				#try:
					#con.login()
					#log.debug("...done.")
				#except: # already logged in
					#log.debug("...already logged in.")
				### retrieve list of all orders within copernicus subscription
				#orders = []
				#con.retrlines("LIST",orders.append) 
				## change order list to just names; strips out username, date, etc.
				#for i in range(len(orders)):
						#orders[i] = orders[i].split(None,8)[-1].lstrip()
				### loop over all orders, searching within each for correctly dated file
				#found=False
				#log.debug("Searching...")
				#for o in orders:
					#log.debug(f"Order: {o}")
					#con.cwd(o)
					#subfolders = []
					#con.retrlines("LIST",subfolders.append) # appends description of each sub-folder
					#for i in range(len(subfolders)):
						#subfolders[i] = subfolders[i].split(None,8)[-1].lstrip() # changes listing to contain just subfolder names
					#files = []
					#fileSizes = []
					## this FOR loop collects the relevant file and size from all subfolders, recursively searching for the one that matches the desired date
					#for sf in subfolders:
						#log.debug(f"\tsubfolder: {sf}")
						#sfDate = sf.split("_")[1][:8]
						#if datetime.strptime(sfDate,"%Y%m%d") == dateObj:
							#log.debug("...product found.")
							#found=True
							#con.cwd(sf) # change working directory to subfolder
							#listing= []
							#con.retrlines("LIST",listing.append)
							#for i in range(len(listing)):
								#fileSizes.append(listing[i].split(None,8)[4].lstrip())
								#files.append(listing[i].split(None,8)[-1].lstrip()) # "files" list now contains names of all files in subfolder
							#break
					#if found: # stop searching if you found a matching file!!
						#return True
					#con.cwd('..') # change working directory back to higher folder
				#return False
			#except ConnectionResetError:
				#log.error("Connection reset error; Copernicus kicked you off")
				#return False

		def checkMod09q1(date:str) -> bool:
			if len(octvi.url.getDates("MOD09Q1",date)) > 0:
				return True
			else:
				return False

		def checkMyd09q1(date:str) -> bool:
			if len(octvi.url.getDates("MYD09Q1",date)) > 0:
				return True
			else:
				return False

		def checkMod13q1(date:str) -> bool:
			if len(octvi.url.getDates("MOD13Q1",date)) > 0:
				return True
			else:
				return False

		def checkMyd13q1(date:str) -> bool:
			if len(octvi.url.getDates("MYD13Q1",date)) > 0:
				return True
			else:
				return False

		if not self.credentials:
			raise NoCredentialsError("Data archive credentials not set. Use 'glamconfigure' on command line to set credentials.")
		actions = {
			'merra-2':checkMerra,
			'chirps':checkChirps,
			'chirps-prelim':checkChirpsPrelim,
			'swi':checkSwi,
			'MOD09Q1':checkMod09q1,
			'MYD09Q1':checkMyd09q1,
			'MOD13Q1':checkMod13q1,
			'MYD13Q1':checkMyd13q1
			}
		return actions[product](date)

	def pullFromSource(self,product:str,date:str,out_dir:str,) -> tuple:
		"""
		This function locates and downloads the requested product x date combination from its source archive if possible
		Downloade files are COGs in sinusoidal projection
		Returns tuple of file paths, or empty tuple if download failed

		...

		Parameters
		----------
		product:str
			string representation of desired product type ('merra-2','chirps','swi')
		date:str
			string representation in format "%Y-%m-%d" of desired product date
		out_dir:str
			path to output directory, where the cloud-optimized geoTiff will be stored
		"""

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
			cloudOpArgs = ["gdal_translate",intermediate_file,in_file,'-q','-co', "TILED=YES",'-co',"COPY_SRC_OVERVIEWS=YES",'-co', "COMPRESS=LZW"]
			subprocess.call(cloudOpArgs)

			## remove intermediate
			os.remove(intermediate_file)

		def downloadMerra2(date:str,out_dir:str) -> tuple:
			"""
			Given date of merra product, downloads file to output directory
			Returns tuple of file paths or empty list if download failed
			Downloaded files are COGs in sinusoidal projection
			"""

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
			merraFiles = {
					'min':[],
					'mean':[],
					'max':[]
				}
 
			## loop over list of urls
			log.debug(m2Urls)
			for url in m2Urls:
				urlDate = url.split(".")[-2]
				## use requests module to download MERRA-2 file (.nc4)
				with requests.Session() as session:
					session.auth = (self.merraUsername, self.merraPassword)
					r1 = session.request('get',url)
					r = session.get(r1.url,auth=(self.merraUsername, self.merraPassword))
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
				sdMin = dataset.GetSubDatasets()[3][0] # full name of T2MMIN subdataset
				sdMax = dataset.GetSubDatasets()[1][0] # full name of T2MMAX subdataset
				sdMean = dataset.GetSubDatasets()[2][0] # full name of T2MMEAN subdataset
				del dataset

				## copy subdatasets to new files
				minOut = os.path.join(out_dir,f"M2_MIN.{urlDate}.TEMP.tif")
				maxOut = os.path.join(out_dir,f"M2_MAX.{urlDate}.TEMP.tif")
				meanOut = os.path.join(out_dir,f"M2_MEAN.{urlDate}.TEMP.tif")
				subprocess.call(["gdal_translate","-q",sdMin,minOut]) # calling the command line to produce the tiff
				subprocess.call(["gdal_translate","-q",sdMax,maxOut]) # calling the command line to produce the tiff
				subprocess.call(["gdal_translate","-q",sdMean,meanOut]) # calling the command line to produce the tiff

				## delete netCDF file
				os.remove(outNc4)

				## append subdataset file paths to corresponding lists in dictionary
				merraFiles['min'].append(minOut)
				merraFiles['mean'].append(meanOut)
				merraFiles['max'].append(maxOut)

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

		def downloadChirps(date:str,out_dir:str) -> tuple:
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
				url = f"ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif.gz"

				## download file at url
				try:
					with open(file_zipped,"w+b") as fz:
						fh = urlopen(Request(url))
						shutil.copyfileobj(fh,fz)
				except URLError:
					log.warning(f"No Chirps file exists for {date}")
					os.remove(file_zipped)
					return ()

				## checksum
				observedSize = int(os.stat(file_zipped).st_size) # size of downloaded file (bytes)
				expectedSize = int(urlopen(Request(url)).info().get("Content-length")) # size anticipated from header (bytes)

				## checksum fails, log warning and return empty list
				if observedSize != expectedSize:
					w=f"WARNING:\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
					log.warning(w)
					os.remove(file_zipped)
					return ()
				
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
					return ()

				## cloud-optimize file
				cloud_optimize_inPlace(file_unzipped)

				## remove corresponding preliminary product, if necessary
				correspondingPrelimFile = os.path.join(out_dir,f"chirps-prelim.{date}.tif")
				if os.path.exists(correspondingPrelimFile):
					os.remove(correspondingPrelimFile)

				## return file path string in tuple
				return tuple([file_unzipped])

			except Exception as e: # catch unhandled error; log warning message; return failure in form of empty tuple
				log.exception(f"Unhandled error downloading chirps for {date}")
				return ()

		def downloadChirpsPrelim(date:str,out_dir:str) -> tuple:
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
				url = f"ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/prelim/global_dekad/tifs/chirps-v2.0.{cYear}.{cMonth}.{cDay}.tif"

				## download file at url
				try:
					with open(tf,"w+b") as fd:
						fs = urlopen(Request(url))
						shutil.copyfileobj(fs,fd)
				except URLError:
					log.warning(f"No Chirps file exists for {date}")
					os.remove(tf)
					return ()

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
					return ()

				## cloud-optimize file
				cloud_optimize_inPlace(file_out)

				## return tuple of file path
				return tuple([file_out])

			except Exception as e: # catch unhandled error; log warning message; return failure in form of empty tuple
				log.exception(f"Unhandled error downloading chirps-prelim for {date}")
				return ()

		def downloadSwi(date:str,out_dir:str) -> tuple:
			"""
			Given date of swi product, downloads file to directory if possible
			Returns tuple containing file path or empty list if download failed
			Downloaded files are COGs in sinusoidal projection
			"""
			# RE-WRITE TO USE HTTP DATA POOL AT https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/

			#https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/10-daily_SWI_12.5km_Global_V3/2019/11/21/SWI10_201911211200_GLOBE_ASCAT_V3.1.1/c_gls_SWI10_201911211200_GLOBE_ASCAT_V3.1.1.nc
			#https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/10-daily_SWI_12.5km_Global_V3/2019/11/11/SWI10_201911111200_GLOBE_ASCAT_V3.1.1/c_gls_SWI10_201911111200_GLOBE_ASCAT_V3.1.1.nc

			out = os.path.join(out_dir,f"swi.{date}.tif")
			dateObj = datetime.strptime(date,"%Y-%m-%d") # convert string date to datetime object
			year = dateObj.strftime("%Y")
			month = dateObj.strftime("%m".zfill(2))
			day = dateObj.strftime("%d".zfill(2))
			
			## download file
			#url = f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/Daily_SWI_12.5km_Global_V3/{year}/{month}/{day}/SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1/c_gls_SWI_201911251200_GLOBE_ASCAT_V3.1.1.nc"
			url = f"https://land.copernicus.vgt.vito.be/PDF/datapool/Vegetation/Soil_Water_Index/Daily_SWI_12.5km_Global_V3/{year}/{month}/{day}/SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1/c_gls_SWI_{year}{month}{day}1200_GLOBE_ASCAT_V3.1.1.nc"
			file_nc = out.replace("tif","nc") # temporary NetCDF file; later to be converted to tiff
			#print(url)

			## use requests module to download MERRA-2 file (.nc4)
			with requests.Session() as session:
				session.auth = (self.swiUsername, self.swiPassword)
				r1 = session.request('get',url)
				r = session.get(r1.url,auth=(self.swiUsername, self.swiPassword))
				headers = r.headers
			# write output .nc file
			with open(file_nc,"wb") as fd: # write data in chunks
				for chunk in r.iter_content(chunk_size = 1024*1024):
					fd.write(chunk)
			#try:
				#with open(file_nc,"w+b") as fz:
					#fh = urlopen(Request(url))
					#shutil.copyfileobj(fh,fz)
			#except URLError:
				#log.warning(f"No SWI file exists for {date}")
				#os.remove(file_nc)
				#return ()

			## checksum
			observedSize = int(os.stat(file_nc).st_size) # size of downloaded file (bytes)
			expectedSize = int(headers['Content-Length']) # size anticipated from header (bytes)

			if int(observedSize) != int(expectedSize):
				w=f"\nExpected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
				log.warning(w)
				os.remove(file_nc)
				return ()

			#try:
				### log into copernicus
				#log.debug("log in to Copernicus...")
				#con = FTP("ftp.copernicus.vgt.vito.be",self.swiUsername,self.swiPassword)
				#try:
					#con.login()
					#log.debug("...done.")
				#except: # already logged in
					#log.debug("...already logged in.")

				### retrieve list of all orders within copernicus subscription
				#orders = []
				#con.retrlines("LIST",orders.append) 
				## change order list to just names; strips out username, date, etc.
				#for i in range(len(orders)):
						#orders[i] = orders[i].split(None,8)[-1].lstrip()

				### loop over all orders, searching within each for correctly dated file
				#found=False
				#log.debug("Searching...")
				#for o in orders:
					#log.debug(f"Order: {o}")
					#con.cwd(o)
					#subfolders = []
					#con.retrlines("LIST",subfolders.append) # appends description of each sub-folder
					#for i in range(len(subfolders)):
						#subfolders[i] = subfolders[i].split(None,8)[-1].lstrip() # changes listing to contain just subfolder names
					#files = []
					#fileSizes = []
					## this FOR loop collects the relevant file and size from all subfolders, recursively searching for the one that matches the desired date
					#for sf in subfolders:
						#log.debug(f"\tsubfolder: {sf}")
						#sfDate = sf.split("_")[1][:8]
						#if datetime.strptime(sfDate,"%Y%m%d") == dateObj:
							#log.debug("...product found.")
							#found=True
							#con.cwd(sf) # change working directory to subfolder
							#listing= []
							#con.retrlines("LIST",listing.append)
							#for i in range(len(listing)):
								#fileSizes.append(listing[i].split(None,8)[4].lstrip())
								#files.append(listing[i].split(None,8)[-1].lstrip()) # "files" list now contains names of all files in subfolder
							#break
					#if found: # stop searching if you found a matching file!!
						#break
					#con.cwd('..') # change working directory back to higher folder

			## sometimes Copernicus gets pissy and breaks the connection; this handling reconnects and tries again
			#except ConnectionResetError:
				#log.exception()
				#return ()

			### download file at url
			
			## break if there is no file
			#if len(files) == 0:
				#w = f"No SWI file exists for {date}"
				#log.warning(w)
				#return ()


			## download the NC file
			#with open(file_nc,"wb") as lf:
				#try:
					#con.retrbinary("RETR "+files[0], lf.write,8*1024) # actually do the downloading
				#except IndexError:
					#w=f"No SWI data for {date}"
					#log.warning(w)
					#lf.close()
					#os.remove(file_nc)
					#return ()
			#con.quit() # break connection

			### checksum
			#observedSize = int(os.stat(file_nc).st_size)
			#expectedSize = int(fileSizes[0])
			#if observedSize != expectedSize: # on checksum failure, skip file
				#w=f"Expected file size:\t{expectedSize} bytes\nObserved file size:\t{observedSize} bytes"
				#log.warning(w)
				#return ()

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
				return ()

			## cloud-optimize file
			cloud_optimize_inPlace(out)

			## return tuple of file path string
			return tuple([out])

		def downloadMod09q1(date:str,out_dir:str) -> tuple:
			"""
			Given date of MOD09Q1 product, downloads file to directory if possible
			Returns tuple containing file path or empty list if download failed
			Downloaded files are COGs in sinusoidal projection
			"""
			product = "MOD09Q1"
			jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

			return tuple([octvi.globalNdvi(product,date,outPath)])

		def downloadMyd09q1(date:str,out_dir:str) -> tuple:
			"""
			Given date of MYD09Q1 product, downloads file to directory if possible
			Returns tuple containing file path or empty list if download failed
			Downloaded files are COGs in sinusoidal projection
			"""
			product = "MYD09Q1"
			jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

			return tuple([octvi.globalNdvi(product,date,outPath)])

		def downloadMod13q1(date:str,out_dir:str) -> tuple:
			"""
			Given date of MOD13Q1 product, downloads file to directory if possible
			Returns tuple containing file path or empty list if download failed
			Downloaded files are COGs in sinusoidal projection
			"""
			product = "MOD13Q1"
			jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

			return tuple([octvi.globalNdvi(product,date,outPath)])

		def downloadMyd13q1 (date:str,out_dir:str) -> tuple:
			"""
			Given date of MYD13Q1 product, downloads file to directory if possible
			Returns tuple containing file path or empty list if download failed
			Downloaded files are COGs in sinusoidal projection
			"""
			product = "MYD13Q1"
			jDate = datetime.strptime(date,"%Y-%m-%d").strftime("%Y.%j")
			outPath = os.path.join(out_dir,f"{product}.{jDate}.tif")

			return tuple([octvi.globalNdvi(product,date,outPath)])

		actions = {
			"swi":downloadSwi,
			"chirps":downloadChirps,
			"chirps-prelim":downloadChirpsPrelim,
			"merra-2":downloadMerra2,
			"MOD09Q1":downloadMod09q1,
			"MYD09Q1":downloadMyd09q1,
			"MOD13Q1":downloadMod13q1,
			"MYD13Q1":downloadMyd13q1
			}
		return actions[product](date=date,out_dir=out_dir)

	def pullFromS3(self,product:str,date:str,out_dir:str,collection=0) -> tuple:
		"""
		Pulls desired product x date combination from S3 bucket and downloads to out_dir
		Downloade files are COGs in sinusoidal projection
		Returns tuple of downloaded product path strings, or empty tuple on failure
		
		...

		Parameters
		----------
		product:str
			string representation of desired product type ('merra-2','chirps','swi')
		date:str
			string representation in format "%Y-%m-%d" of desired product date
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
		else:
			year = datetime.strptime(date,"%Y-%m-%d").strftime("%Y")
			doy = datetime.strptime(date,"%Y-%m-%d").strftime("%j")
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

	def getAllS3(self,product=None,year=None,doy=None) -> list:
		"""
		Queries the database to find all imagery available on S3 that matches
		requested product and/or date. If neither is passed, a list of all
		imagery is returned--beware its size!

		Return value is a list of tuples, in format ("PRODUCT","DATE")

		***

		Parameters
		----------
		product:str
			Name of desired product, e.g. "MOD13Q1"
		year:str
			year of desired imagery
		doy:str
			Day of year of desired imagery
		"""
		if self.noCred:
			log.warning("Database credentials not found. 'getAllS3' is unavailable. Use 'glamconfigure' on command line to set archive credentials.")
			return None
			
		with self.engine.begin() as connection:
			query = "SELECT product, year, day FROM datasets WHERE type = 'image' AND"
			if product:
				query += " product = '%s' AND" % (product)
			if year:
				year = str(year)
				query += " year = '%s' AND" % (year)
			if doy:
				doy = str(doy).zfill(3)
				query += " day = '%s' AND" % (doy)
			query = query.strip().strip(query.strip().split(" ")[-1]).strip() + ";"
			result = connection.execute(query).fetchall()
		output = []
		for t in result:
			resultProduct, resultYear, resultDoy = t
			resultDate = datetime.strptime(f"{resultYear}.{resultDoy}","%Y.%j").strftime("%Y-%m-%d")
			output.append(tuple([resultProduct,resultDate]))
		return output

	def listMissing(self,directory:str) -> list:
		"""
		Compares database listing of all available S3 imagery to files in a 
		given directory. Returns a list of those which are on S3 but not on
		disk ("missing files").

		Return value is a list of tuples, in format ("PRODUCT","DATE")

		***

		Parameters
		----------
		directory:str
			Full path to directory where imagery is stored. Image
			files must be "well-named"; that is, "PROUDUCT.YYYY-MM-DD.tif"
			for ancillary files, and "PRODUCT.YYYY.JJJ.tif" for NDVI
			files.
		"""
		dir_files = glob.glob(os.path.join(directory,"*.tif"))
		dir_products = []
		dir_dates = []
		for f in dir_files:
			parts = os.path.basename(f).split(".")
			product = parts[0]
			try:
				if product in octvi.supported_products:
					date = datetime.strptime(f"{parts[1]}.{parts[2]}","%Y.%j").strftime("%Y-%m-%d")
				else:
					date = datetime.strptime(parts[1],"%Y-%m-%d").strftime("%Y-%m-%d") # check for correct formatting
			except:
				log.error(f"Date format in {f} not recognized.")
				continue
			dir_products.append(product)
			dir_dates.append(date)
		# check that there's exactly one product in the directory
		dir_products = set(dir_products)
		if len(dir_products) == 0:
			log.error(f"Found no existing imagery. Use Downloader.getAllS3() to list all available imagery.")
			return None
		if len(dir_products) != 1:
			log.error(f"Found more than 1 unique product ({len(dir_products)})")
			return None
		dir_product = dir_products.pop() # get element from dir_products
		files_onDisk = [(dir_product,d) for d in dir_dates]
		files_onS3 = self.getAllS3(product=dir_product)
		files_missing = [t for t in files_onS3 if t not in files_onDisk]
		return files_missing


# feed a downloaded file path string into this baby's constructor, and let 'er rip. Handles all ingestion, status checks, stats generation, and stats uploading
class Image:
	"""
	A class used to represent a data file for the GLAM system

	...

	Attributes
	----------
	engine: sqlalchemy engine object
		this database engine is connected to the glam system database
	metadata: sqlalchemy metadata object
		stores the metadata
	masks: sqlalchmy table object
		a look-up table storing id values for each crop mask
	regions: sqlalchmy table object
		a look-up table storing id values for each admin level
	products: sqlalchmy table object
		a look-up table storing id values for each product type, both ancillary and modis
	stats: sqlalchemy table object
		a look-up table linking produc, region, and mask id combinations to their corresponding stats table id
	product_status: sqlalchemy table object
		a table recording the extent to which the image has been processed into the glam system
	admins: list
		string representations of the supported admin levels, including gaul global and individual countries
	path: str
		full path to input raster file
	product: str
		type of data product, extracted from file path
	date: str
		full date of input raster file, extracted from file path
	year: str
		year of input raster file, extracted from date
	doy: str
		day of year of input raster file, converted from date
	cropMaskFiles: dict
		dictionary which links the five considered crops to their corresponding crop mask raster
	adminFiles: dict
		dictionary which links the admin levels to their corresponding admin zone raster


	Methods
	-------
	getStatus() -> dict
		Returns dictionary of product status: {'downloade':bool,'processed':bool,'statGen':bool}
	setStatus(stage,status) -> None
		Writes new status of image to database
	isProcessed() -> bool
		Returns whether the file has been uploaded to the database and S3 bucket, according to the product_status table
	statsGenerated() -> bool
		Returns whether statistics have been generated and uploaded to the database, according to the product_status table
	ingest() -> None
		Uploads the file at self.path to the aws s3 bucket, and inserts the corresponding base file name into the database
	getStatsTables() -> dict
		Returns a nested dictionary, organized by crop and admin; result[crop][admin] -> StatsTable object with attributes name:str and exists:bool
	uploadStats() -> None
		Calculates and uploads all statistics for the given data file to the database 
	"""

	# mysql credentials
	noCred = None
	try:
		mysql_user = os.environ['glam_mysql_user']
		mysql_pass = os.environ['glam_mysql_pass']
		mysql_db = 'modis_dev'

		engine = db.create_engine(f'mysql+pymysql://{mysql_user}:{mysql_pass}@glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com/{mysql_db}')
		Session = sessionmaker(bind=engine)
		metadata = db.MetaData()
		masks = db.Table('masks', metadata, autoload=True, autoload_with=engine)
		regions = db.Table('regions', metadata, autoload=True, autoload_with=engine)
		products = db.Table('products', metadata, autoload=True, autoload_with=engine)
		stats = db.Table('stats', metadata, autoload=True, autoload_with=engine)
		product_status = db.Table('product_status',metadata,autoload=True,autoload_with=engine)
	except KeyError:
		log.warning("Database credentials not found. Image objects cannot be instantialized. Use 'glamconfigure' on command line to set archive credentials.")
		noCred = True


	def __init__(self,file_path:str):
		if self.noCred:
			raise NoCredentialsError("Database credentials not found. Image objects cannot be instantialized. Use 'glamconfigure' on command line to set archive credentials.")
		self.type = "image"
		if not os.path.exists(file_path):
			raise BadInputError(f"File {file_path} not found")
		self.path = file_path
		self.product = os.path.basename(file_path).split(".")[0]
		if self.product not in ancillary_products:
			raise BadInputError(f"Product type '{self.product}' not recognized")
		if self.product == 'merra-2':
			merraCollections = {'min':'Minimum','mean':'Mean','max':'Maximum'}
			self.collection = merraCollections.get(self.path.split(".")[-2],'0')
		else:
			self.collection = '0'
		self.date = os.path.basename(file_path).split(".")[1]
		try:
			self.year = self.date.split("-")[0]
			self.doy = datetime.strptime(self.date,"%Y-%m-%d").strftime("%j")
		except:
			raise BadInputError("Incorrect date format in file name. Format is: '{product}.{yyyy-mm-dd}.tif'")
		self.admins = admins
		self.crops = crops
		self.cropMaskFiles = {crop:os.path.join(os.path.dirname(os.path.abspath(__file__)),"statscode","Masks",f"{self.product}.{crop}.tif") for crop in self.crops}
		self.cropMaskFiles['nomask'] = None
		self.adminFiles = {level:os.path.join(os.path.dirname(os.path.abspath(__file__)),f"statscode","Regions",f"{self.product}.{level}.tif") for level in self.admins}


	def __repr__(self):
		return f"<Instance of Image, product:{self.product}, date:{self.date}, collection:{self.collection}, type:{self.type}>"

	def __gt__(self,other):
		"""Compare by date"""
		try:
			return True if datetime.strptime(self.date,"%Y-%m-%d") > other else False # check whether date of instance is later than other
		except TypeError: # is it expecting a datetime.date?
			try:
				return True if datetime.date(datetime.strptime(self.date,"%Y-%m-%d")) > other else False # check whether date of instance is later than other
			except TypeError: # is the other value a string?
				return True if datetime.strptime(self.date,"%Y-%m-%d") > datetime.strptime(other,"%Y-%m-%d") else False # check whether date of instance is later than datetime(other)

	def __lt__(self,other):
		"""Compare by date"""
		try:
			return True if datetime.strptime(self.date,"%Y-%m-%d") < other else False
		except TypeError: # is it expecting a datetime.date?
			try:
				return True if datetime.date(datetime.strptime(self.date,"%Y-%m-%d")) < other else False
			except TypeError: # is the other value a string?
				return True if datetime.strptime(self.date,"%Y-%m-%d") < datetime.strptime(other,"%Y-%m-%d") else False

	def getStatus(self) -> dict:
		"""
		Returns dictionary of status flags, as stored in product_status table in database
		Return structure: {downloaded:bool,processed:bool,statsGen:bool}
		"""
		pass
		o = {}
		query = db.select(['*']).where(self.product_status.columns.product==self.product).where(self.product_status.columns.date==self.date)
		with self.engine.begin() as connection:
			r = connection.execute(query).fetchone()
		try:
			o["downloaded"] = True if r[3] else False
			o["processed"] = True if r[4] else False
			o["statGen"] = True if r[6] else False
		except (IndexError,TypeError):
			#raise RecordNotFoundError(f"No record in table `product_status` for {self.product} on {self.date}")
			with self.engine.begin() as connection:
				connection.execute(f"INSERT INTO product_status (product, date, downloaded, processed, completed) VALUES ('{self.product}', '{self.date}', 1, 0, 0);")
			return(self.getStatus())
		return o

	def setStatus(self, stage:str, status:bool) -> None:
		assert stage in ['downloaded','processed','statGen']
		#cmd = db.update(self.product_status).where(self.product_status.c.product==self.product).where(self.product_status.c.date==self.date).values(stage=status)
		cmd = f"UPDATE product_status SET {stage} = {status} WHERE product = '{self.product}' AND date = '{self.date}';"
		with self.engine.begin() as connection:
			connection.execute(cmd)

	def isProcessed(self) -> bool:
		"""Returns whether the file has been uploaded to the database and S3 bucket, according to the product_status table"""
		with self.engine.begin() as connection:
			query = db.select([self.product_status.columns.processed]).where(self.product_status.columns.product==self.product).where(self.product_status.columns.date==self.date)
			r = connection.execute(query).fetchone()
			#r = connection.execute(f"SELECT processed FROM product_status WHERE product='{self.product}' AND date='{self.date}';").fetchone()
		if r[0] == 1:
			return True
		elif r[0] == 0:
			return False
		else:
			return None

	def statsGenerated(self) -> bool:
		"""
		Returns whether statistics have been generated and uploaded to the database, according to the product_status table
		"""
		with self.engine.begin() as connection:
			query = db.select([self.product_status.columns.statGen]).where(self.product_status.columns.product==self.product).where(self.product_status.columns.date==self.date)
			r = connection.execute(query).fetchone()
		if r is None:
			log.error(f"No record found in product_status table for product='{self.product}' and date='{self.date}'")
			return None
		if r[0] == 1:
			return True
		elif r[0] == 0:
			return False
		else:
			return None

	def ingest(self) -> bool:
		"""
		Uploads the file at self.path to the aws s3 bucket, and inserts the corresponding base file name into the database
		Returns True on success and False on failure
		"""

		log.debug("-defining variables")
		file_name = os.path.basename(self.path) # extracts directory of image file
		s3_bucket = 'glam-tc-data/rasters/' # name of s3 bucket
		# mysql credentials
		try:
			mysql_user = os.environ['glam_mysql_user']
			mysql_pass = os.environ['glam_mysql_pass']
			mysql_db = 'modis_dev'
		except KeyError:
			raise NoCredentialsError("Database credentials not found. Use 'glamconfigure' on command line to set archive credentials.")

		rds_endpoint = 'glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com'
		mysql_path = 'mysql://'+mysql_user+':'+mysql_pass+'@'+rds_endpoint+'/'+mysql_db # full path to mysql database

		## update database
		log.debug("-adding file to database")
		driver = tc.get_driver(mysql_path)
		key_names = ('product', 'year', 'day','collection','type')

		# inserting file into database
		keys = {"product":self.product,'collection':self.collection,"year":self.year,"day":self.doy,"type":self.type}
		log.debug(keys)
		s3_path = 's3://'+s3_bucket+file_name
		try:
			driver.insert(keys=keys, filepath=self.path, override_path=f'{s3_path}')
		except Exception as e:
			log.error(e)
			return False

		## upload file to s3 bucket
		log.debug("-uploading file to s3 bucket")
		def upload_file_s3(upload_file,bucket) -> bool:
			try:
				s3_client = boto3.client('s3',
					aws_access_key_id=os.environ['AWS_accessKeyId'],
					aws_secret_access_key=os.environ['AWS_secretAccessKey']
					)
			except KeyError:
				raise NoCredentialsError("Amazon Web Services (AWS) credentials not found. Use 'aws configure' on the command line.")
			
			b = bucket.split("/")[0]
			k = bucket.split("/")[1]+"/"+os.path.basename(upload_file)
			try:
				response = s3_client.upload_file(Filename=upload_file,Bucket=b,Key=k)
			except ClientError as e:
				log.error(e)
				return False
			return True
		u = upload_file_s3(self.path,s3_bucket)

		## on success, update database to match
		if u and (self.type == 'image'):
			updateSql = f"UPDATE product_status SET processed = True WHERE product = '{self.product}' AND date = '{self.date}';"
			with self.engine.begin() as connection:
				try:
					x = connection.execute(updateSql)
					if x.rowcount == 0:
						connection.execute(f"INSERT INTO product_status (product, date, downloaded, processed, completed, statGen) VALUES ('{self.product}','{self.date}',True,True,False,False);")
				except db.exc.OperationalError:
					return False

		## return True if everything succeeded, or False otherwise
		return u

	def getStatsTables(self) -> dict:
		"""
		Used in __init__ to generate nested dictionary of stats table names and statuses (whether they exist)
		Dictionary structure is
			{crop:{admin:StatsTable(name:str,exists:bool)}},
		where StatsTable is an object created with the collections.namedtuple() factory
		"""
		StatsTable = collections.namedtuple("StatsTable","name exists")

		@log_io
		def idCheck(variable, value,collection = None) -> int:
			"""
			Pass variable [product, mask, region] and desired value
			Returns corresponding ID for use in look-up table `stats`
			"""
			if variable == 'product':
				var_id = self.products.columns.product_id
				var_name = self.products.columns.name
			elif variable == 'mask':
				var_id = self.masks.columns.mask_id
				var_name = self.masks.columns.name
			elif variable == 'region':
				var_id = self.regions.columns.region_id
				var_name = self.regions.columns.name

			query = db.select([var_id])\
							.where(var_name == value)
			if collection:
				query = query.where(self.products.columns.collection == collection)
			with self.engine.begin() as connection:
				result = connection.execute(query).fetchall()
			try:
				if len(result) <1:
					err_msg = f"No record for {value} exists in {variable} table"
					raise BadInputError(err_msg)
			except BadInputError as e:
				log.exception("BadInputError")

			return result[0][0]


		@log_io
		def getStatID(product_id, mask_id, region_id) -> collections.namedtuple:
			"""
			Pass id values for product, mask, and region as they appear in the 'stats' table.
			Returns StatsTable object with the following fields:
				name: str
					full name of corresponding stats table in database, formatted as "stats_{id}"
				exists: bool
					whether or not the table currently exists in the database, or is the next highest ID
			"""
			# the query below selects column `stats_id` where the other four fields match the input
			stat_query = db.select([self.stats.columns.stats_id,self.stats.columns.created])\
							.select_from(self.stats.join(self.products).join(self.masks).join(self.regions))\
							.where(self.products.columns.product_id == product_id)\
							.where(self.masks.columns.mask_id == mask_id)\
							.where(self.regions.columns.region_id == region_id)\
							.where(self.stats.columns.year == self.year)

			# there's been some confusion over whether to use Session or Connection objects. Still unresolved.
			session = self.Session()
			#with self.engine.begin() as connection:
			try:
				# try to get the stats_id that matches the product, crop, admin, and year
				stat_result = session.execute(stat_query).fetchall()

				try: # check whether the given stats table already exists
					session.execute(f"SELECT admin FROM stats_{stat_result[0][0]};").fetchone()
					return StatsTable(f"stats_{stat_result[0][0]}", True)

				except IndexError: # thrown if there IS NO matching stats_id--need to create a new record in the stats table
					#newHighestID = session.execute(func.max(self.stats.columns.stats_id)).fetchall()[0][0] + 1
					#session.execute(self.stats.insert().values(stats_id=newHighestID,product_id=product_id,mask_id=mask_id,region_id=region_id,year=self.year)) # insert record into 'stats' LUT; ensures that repeated method calls do not return duplicate new stats id numbers
					session.execute(self.stats.insert().values(product_id=product_id,mask_id=mask_id,region_id=region_id,year=self.year)) # insert record into 'stats' LUT; `stats_id` field is auto-incrementing to prevent duplicates
					return getStatID(product_id,mask_id,region_id)

				except db.exc.ProgrammingError: # thrown if the record in 'stats' exists, but the actual 'stats_EXAMPLE' table does not. Just returns a "false" in the `created` field of the StatsTable object result
					return StatsTable(f"stats_{stat_result[0][0]}",False)

				session.commit()
			except:
				session.rollback()
				raise
			finally:
				session.close()

		product_id = idCheck("product",self.product,self.collection.lower())
		
		return {crop:{admin:getStatID(product_id,idCheck('mask',crop),idCheck('region',admin)) for admin in self.admins} for crop in self.crops} # nested dictionary: first level = crops, second level = admins


	def uploadStats(self,stats_tables = None,admin_level="ALL",crop_level="ALL",override_brazil_limit=False) -> None:
		"""
		Calculates and uploads all statistics for the given data file to the database

		Description
		-----------
		For each crop x admin combination, a pandas dataframe of statistics by region is created
		These combinations are then paired up with the corresponding stats table name, as found in stats_tables
		For each table:
			If the table does not exist (table.exists==False) then it is created
			The dataframe is uploaded to the table
		If all stats are successfully uploaded, the function updates product_status.statGen to True

		***

		Parameters
		----------
		stats_tables:dict
			Nested dictionary of stats table IDs. Create with getStatsTables()
		admin_level:str
			One of "ALL", "GAUL", or "BRAZIL". Defines which admin levels will
			have statistics run. Allows for running only certain combinations.
		crop_level:str
			One of "ALL", "NOMASK", "CROPMONITOR", or "BRAZIL". Defines which crop masks will
			have statistics run. Allows for running only certain combinations.
		override_brazil_limit:bool
			If False (default), only run brazil crops for brazil regions. If True, runs brazil
			crops for ALL regions.
		"""
		def zonal_stats(image_path:str, crop_mask_path:str, admin_path:str) -> 'pandas.DataFrame':
			"""
			Generate pandas dataframe of statistics for a given combination of image x mask x admins
			Returns a pandas dataframe of statistics by admin ID: arable pixels, clear pixels, percent clear, and mean of image raster within clear arable pixels.
			...

			Parameters
			----------
			image_path:str
				file path to a data raster image
			crop_mask_path:str
				file path to a crop mask (binary raster image of same resolution as data)
			admin_path:str
				administrative region path (categorical raster of same resolution)
			"""

			#Process in tile sized batches
			GA_ReadOnly = 0

			def isBrazil(admin_path) -> bool:
				level = os.path.basename(admin_path).split(".")[0]
				if level.split("_")[0] == "BR":
					return True
				else:
					return False

			def getValidWindow(dataset,bandhandle,nodata_value) -> "Tuple of (xmin,ymin,xmax,ymax)":
				arr = BandReadAsArray(bandhandle)
				for i in range(0,dataset.RasterYSize,1):
					if not np.all(arr[i,:]==nodata_value):
						ymax = i
						break
				for j in range(0,dataset.RasterXSize,1):
					if not np.all(arr[:,j]==nodata_value):
						xmax = j
						break
				for i in range(dataset.RasterYSize-1,-1,-1):
					if not np.all(arr[i,:]==nodata_value):
						ymin = i
						break
				for j in range(dataset.RasterXSize-1,-1,-1):
					if not np.all(arr[:,j]==nodata_value):
						xmin=j
						break
				return (xmin,ymin,xmax,ymax)
			
			xBSize = 256
			yBSize = 256
			stats = []
			flatarrays = {}

			log.debug(f'running: {admin_path}, {crop_mask_path}, {image_path}')
			### Open the admin unit file first, should be a tif file
			### No admin unit = 0 value
			### Everything is pixel based and preprocessed, no need to worry about the geotransforms
			adminds = gdal.Open(admin_path, GA_ReadOnly)
			#assert adminds
			adminbandhandle = adminds.GetRasterBand(1)
			adminnodata = adminbandhandle.GetNoDataValue()

			### Open the crop mask file, should also be a tif file
			### No crop = 0 (no data), 1 = crop
			if crop_mask_path:
				cmds = gdal.Open(crop_mask_path, GA_ReadOnly)
				#assert cmds
				cmbandhandle = cmds.GetRasterBand(1)
				cmnodata = cmbandhandle.GetNoDataValue()
			else:
				cmbandhandle=None
				cmnodata = 0

			### Open the ndvi file, should be a tif file
			ndvids = gdal.Open(image_path, GA_ReadOnly)
			#assert ndvids
			### The name of the NDVI band for C6 data:
			ndvibandhandle = ndvids.GetRasterBand(1)
			ndvinodata = ndvibandhandle.GetNoDataValue()
			rows = ndvids.RasterYSize
			cols = ndvids.RasterXSize

			if isBrazil(admin_path):
				##Execution for BR_Mesoregion, BR_Microregion, BR_Municipality, BR_State
				adminWindow = getValidWindow(adminds,adminbandhandle,adminnodata)
				xOffset = adminWindow[2]
				yOffset = adminWindow[3]
				numCols = adminWindow[0]-adminWindow[2]
				numRows = adminWindow[1]-adminWindow[3]

				# windowed read of each dataset
				adminband = adminbandhandle.ReadAsArray(xOffset, yOffset, numCols, numRows) # starts at I and J continues for nC and nR
				try:
					cmband = cmbandhandle.ReadAsArray(xOffset, yOffset, numCols, numRows)
				# if there is no crop mask, just calculate all pixels
				except:
					cmband = np.full((numRows,numCols),1)
				ndviband = ndvibandhandle.ReadAsArray(xOffset, yOffset, numCols, numRows)
				##print(adminband.shape)
				# Loop over the unique values in the admin layer
				uniqueadmins = np.unique(adminband[adminband != adminnodata])
				# Loop through admin units, skip 0
				for adm in uniqueadmins:
					thisadm = str(adm)
					# Mask the source data array with our current feature
					# we also mask out nodata values explictly
					statcountarable = int((adminband[(adminband == adm) & (cmband != cmnodata)]).size)
					if statcountarable == 0:
						continue
					masked = np.array(ndviband[(ndviband != ndvinodata) & (cmband != cmnodata) & (adminband == adm)], dtype='int64')
					statcount = masked.size
					if thisadm not in flatarrays:
						flatarrays[thisadm] = {
							'values': (masked.mean() if (statcount > 0) else 0),
							'count': statcount,
							'countarable' : statcountarable
						}
					else:
						updatedcount = flatarrays[thisadm]['count'] + statcount
						if updatedcount > 0:
							if np.isnan(np.sum(flatarrays[thisadm]['values'])):
								flatarrays[thisadm]['values'] = masked.sum() / updatedcount
							else:
								flatarrays[thisadm]['values'] = ((flatarrays[thisadm]['values'] * flatarrays[thisadm]['count']) + masked.sum()) / updatedcount
							flatarrays[thisadm]['count'] = updatedcount
						else:
							flatarrays[thisadm]['count'] = 0
							flatarrays[thisadm]['values'] = 0
						flatarrays[thisadm]['countarable'] += statcountarable
			else:
				## Execution for gaul1, etc.
				for i in range(0, rows, yBSize):
					if ((i + yBSize) < rows):
						numRows = yBSize
					else:
						numRows = rows - i
					for j in range(0, cols, xBSize):
						if ((j + xBSize) < cols):
							numCols = xBSize
						else:
							numCols = cols - j
						# Process each block here
						adminband = adminbandhandle.ReadAsArray(j, i, numCols, numRows)
						try:
							cmband = cmbandhandle.ReadAsArray(xOffset, yOffset, numCols, numRows)
						# if there is no crop mask, just calculate all pixels
						except:
							cmband = np.full((numRows,numCols),1)
						ndviband = ndvibandhandle.ReadAsArray(j, i, numCols, numRows)
						##print(adminband.shape)
						# Loop over the unique values in the admin layer
						uniqueadmins = np.unique(adminband[adminband != adminnodata])
						# Loop through admin units, skip 0
						for adm in uniqueadmins:
							thisadm = str(adm)
							# Mask the source data array with our current feature
							# we also mask out nodata values explictly
							statcountarable = int((adminband[(adminband == adm) & (cmband != cmnodata)]).size)
							if statcountarable == 0:
								continue
							masked = np.array(ndviband[(ndviband != ndvinodata) & (cmband != cmnodata) & (adminband == adm)], dtype='int64')
							statcount = masked.size
							if thisadm not in flatarrays:
								flatarrays[thisadm] = {
									'values': (masked.mean() if (statcount > 0) else 0),
									'count': statcount,
									'countarable' : statcountarable
								}
							else:
								updatedcount = flatarrays[thisadm]['count'] + statcount
								if updatedcount > 0:
									if np.isnan(np.sum(flatarrays[thisadm]['values'])):
										flatarrays[thisadm]['values'] = masked.sum() / updatedcount
									else:
										flatarrays[thisadm]['values'] = ((flatarrays[thisadm]['values'] * flatarrays[thisadm]['count']) + masked.sum()) / updatedcount
									flatarrays[thisadm]['count'] = updatedcount
								else:
									flatarrays[thisadm]['count'] = 0
									flatarrays[thisadm]['values'] = 0
								flatarrays[thisadm]['countarable'] += statcountarable

			alladms = list(flatarrays.keys())
			for finaladm in alladms:
				values = flatarrays[finaladm]['values']
				count = flatarrays[finaladm]['count']
				arable_count = flatarrays[finaladm]['countarable']
				try:
					feature_stats = {
						'value': round(float(values),2),
						'count': round(float(count),2),
						'arable': round(float(arable_count),2),
						'pct': np.floor(float(count) / float(arable_count) * 100),
						'admin': finaladm
					}
					stats.append(feature_stats)
				except ValueError: #Array size is zero, do nothing
					log.warning("No pixels found for admin zone: {}".format(adm))
			try:
				header = list(stats[0].keys())
				header.sort()
			except IndexError: #Ag mask doesn't overlap admin mask (e.g. Brazil x SpringWheat)
				log.warning(f"No mask-region overlap for {crop_mask_path} and {admin_path}")
				return None

			sortedData = {}
			for v in header:
				sortedData[v] = []
			for stat in stats:
				for k in stat.keys():
					sortedData[k].append(stat[k])
			return pd.DataFrame(sortedData)

		@log_io
		def create_stats_table(table_name:str,df:'pandas.DataFrame') -> None:
			newCol_val = f"val.{self.doy}"
			newCol_pct = f"pct.{self.doy}"
			df=df.rename(columns={"value":newCol_val,"pct":newCol_pct})
			df_subset = df[["admin","arable",newCol_val,newCol_pct]]
			log.info(f"--new table: {table_name}")
			with self.engine.begin() as connection:
				connection.execute(f"CREATE TABLE {table_name} (`admin` int, `arable` int, `{newCol_val}` float(2), `{newCol_pct}` float(2));") # create empty table with correct columns for one day's worth of data
				df_subset.to_sql(f"{table_name}",self.engine,if_exists='append',index=False) # add data as rows to the newly-created table
				connection.execute(f"CREATE INDEX index_{table_name} on {table_name}(admin);") # create index on admin column for faster lookups

		def append_to_stats_table(table_name:str,df:'pandas.DataFrame') -> None:
			"""
			Given a stats table name and a pandas dataframe, first checks to see whether the desired columns exist; if not, creates them and fills them with correct stats values
			...

			Parameters
			----------
			table_name:str
				name of the stats table, in format "stats_{stats_id}"
			df:pandas.DataFrame
				pandas dataframe of statistics for the image x mask x admin combination in question
			"""
			newCol_val = f"val.{self.doy}"
			newCol_pct = f"pct.{self.doy}"
			with self.engine.begin() as connection:
				try:
					connection.execute(f"SELECT `{newCol_val}` FROM {table_name}") # try to select the desired columns
					log.debug(f"-column {newCol_val} already exists in table {table_name}")
				except db.exc.InternalError: # if the column does not exist, the attempt to select it will throw an error
					# alter the table to add the desired columns
					aSql = f"ALTER TABLE {table_name} ADD `{newCol_val}` float(2)"
					connection.execute(aSql)
				try:
					connection.execute(f"SELECT `{newCol_pct}` FROM {table_name}") # as above, but for newcol_pct
					log.debug(f"-column {newCol_pct} already exists in table {table_name}")
				except db.exc.InternalError:
					aSql = f"ALTER TABLE {table_name} ADD `{newCol_pct}` float(2)"
					connection.execute(aSql)
				for index, row in df.iterrows():
					uSql= f"UPDATE {table_name} SET `{newCol_val}`={row['value']} WHERE admin = {row['admin']}"
					connection.execute(uSql)
					uSql= f"UPDATE {table_name} SET `{newCol_pct}`={row['pct']} WHERE admin = {row['admin']}"
					connection.execute(uSql)

		retries = 0
		try:
			if stats_tables is None:
				stats_tables = self.getStatsTables()
			for crop in stats_tables.keys():
				if crop_level == "NOMASK" and crop != "nomask":
					continue
				if crop_level == "BRAZIL" and crop not in crops_brazil:
					continue
				if crop_level == "CROPMONITOR" and crop not in crops_cropmonitor:
					continue
				for admin in stats_tables[crop].keys():
					if admin_level == "BRAZIL" and admin not in admins_brazil:
						continue
					if admin_level == "GAUL" and admin not in admins_gaul:
						continue
					if not override_brazil_limit:
						if crop in crops_brazil and admin not in admins_brazil:
							continue
					log.info(f"{crop} x {admin}")
					statsTable = stats_tables[crop][admin] # extract correct StatsTable object, with fields .name:str and .exists:bool
					try:
						statsDataFrame = zonal_stats(self.path,self.cropMaskFiles[crop],self.adminFiles[admin]) # generate data frame of statistics
					except RuntimeError: # no such crop or admin file
						log.exception("Missing crop mask or admin zone raster.")
						continue
					if statsDataFrame is not None: # check if zonal_stats returned a dataframe or None
						if statsTable.exists: # if the stats table already exists, append the new columns to it
							append_to_stats_table(statsTable.name,statsDataFrame)
						else: # if the stats table does not exist, create it with the stats information already in
							create_stats_table(statsTable.name,statsDataFrame)
					else: # if zonal_stats returned None, that means the combination of crop/admin is invalid (for example, there is no overlap beetween spring wheat and the Brazil masks)
						continue
		except db.exc.OperationalError: # sometimes, the database just randomly conks out. No idea why. This restarts the attempt as many times as needed. Watch out for rogue loops.
			if retries <= 3:
				log.warning("WARNING: Lost connection to database. Trying again.")
				self.uploadStats(stats_tables)
			else:
				log.warning("WARNING: Lost connection to database, 3 retries used up. Skipping.")
				return "Stats not generated. Database connection lost 4 times in a row. Ouch."

		## update product_status if all stats uploaded
		if admin_level == "ALL" and crop_level == "ALL":
			with self.engine.begin() as connection:
				updateSql = f"UPDATE product_status SET statGen = True WHERE product = '{self.product}' AND date = '{self.date}';"
				x = connection.execute(updateSql)
				if x.rowcount == 0:
					connection.execute(f"INSERT INTO product_status (product, date, downloaded, processed, completed, statGen) VALUES ('{self.product}','{self.date}',True,False,False,True);")

		return True


class AncillaryImage(Image):
	"""
	A class used to represent an ancillary data file for the GLAM system

	...

	Attributes
	----------
	engine: sqlalchemy engine object
		this database engine is connected to the glam system database
	metadata: sqlalchemy metadata object
		stores the metadata
	masks: sqlalchmy table object
		a look-up table storing id values for each crop mask
	regions: sqlalchmy table object
		a look-up table storing id values for each admin level
	products: sqlalchmy table object
		a look-up table storing id values for each product type, both ancillary and modis
	stats: sqlalchemy table object
		a look-up table linking produc, region, and mask id combinations to their corresponding stats table id
	product_status: sqlalchemy table object
		a table recording the extent to which the image has been processed into the glam system
	admins: list
		string representations of the supported admin levels, including gaul global and individual countries
	path: str
		full path to input raster file
	product: str
		type of data product, extracted from file path
	date: str
		full date of input raster file, extracted from file path
	year: str
		year of input raster file, extracted from date
	doy: str
		day of year of input raster file, converted from date
	cropMaskFiles: dict
		dictionary which links the five considered crops to their corresponding crop mask raster
	adminFiles: dict
		dictionary which links the admin levels to their corresponding admin zone raster


	Methods
	-------
	getStatus() -> dict
		Returns dictionary of product status: {'downloade':bool,'processed':bool,'statGen':bool}
	setStatus(stage,status) -> None
		Writes new status of image to database
	isProcessed() -> bool
		Returns whether the file has b
		een uploaded to the database and S3 bucket, according to the product_status table
	statsGenerated() -> bool
		Returns whether statistics have been generated and uploaded to the database, according to the product_status table
	ingest() -> None
		Uploads the file at self.path to the aws s3 bucket, and inserts the corresponding base file name into the database
	getStatsTables() -> dict
		Returns a nested dictionary, organized by crop and admin; result[crop][admin] -> StatsTable object with attributes name:str and exists:bool
	uploadStats() -> None
		Calculates and uploads all statistics for the given data file to the database 
	"""
	def __repr__(self):
		return f"<Instance of AncillaryImage, product:{self.product}, date:{self.date}, collection:{self.collection}, type:{self.type}>"
	pass


class ModisImage(Image):
	"""
	A class used to represent an NDVI data file from the MODIS sensor on either the Terra or Aqua satellite

	...

	Attributes
	----------
	engine: sqlalchemy engine object
		this database engine is connected to the glam system database
	metadata: sqlalchemy metadata object
		stores the metadata
	masks: sqlalchemy table object
		a look-up table storing id values for each crop mask
	regions: sqlalchmy table object
		a look-up table storing id values for each admin level
	products: sqlalchmy table object
		a look-up table storing id values for each product type, both ancillary and modis
	stats: sqlalchemy table object
		a look-up table linking product, region, and mask id combinations to their corresponding stats table id
	product_status: sqlalchemy table object
		a table recording the extent to which the image has been processed into the glam system
	admins: list
		string representations of the supported admin levels, including gaul global and individual countries
	path: str
		full path to input raster file
	product: str
		type of data product, extracted from file path
	date: str
		full date of input raster file, extracted from file path
	year: str
		year of input raster file, extracted from date
	doy: str
		day of year of input raster file, converted from date
	cropMaskFiles: dict
		dictionary which links the five considered crops to their corresponding crop mask raster
	adminFiles: dict
		dictionary which links the admin levels to their corresponding admin zone raster

	Methods
	-------
	getStatus() -> dict
		Returns dictionary of product status: {'downloade':bool,'processed':bool,'statGen':bool}
	setStatus(stage,status) -> None
		Writes new status of image to database
	isProcessed() -> bool
		Returns whether the file has been uploaded to the database and S3 bucket, according to the product_status table
	statsGenerated() -> bool
		Returns whether statistics have been generated and uploaded to the database, according to the product_status table
	ingest() -> None
		Uploads the file at self.path to the aws s3 bucket, and inserts the corresponding base file name into the database
	getStatsTables() -> dict
		Returns a nested dictionary, organized by crop and admin; result[crop][admin] -> StatsTable object with attributes name:str and exists:bool
	uploadStats() -> None
		Calculates and uploads all statistics for the given data file to the database 
	"""

	# mysql credentials
	#try:
		#mysql_user = os.environ['glam_mysql_user']
		#mysql_pass = os.environ['glam_mysql_pass']
		#mysql_db = 'modis_dev'
	#except KeyError:
		#log.warning("Database credentials not found. ModisImage objects cannot be instantialized. Use 'glamconfigure' on command line to set archive credentials.")
	#engine = db.create_engine(f'mysql+pymysql://{mysql_user}:{mysql_pass}@glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com/{mysql_db}')
	#metadata = db.MetaData()
	#masks = db.Table('masks', metadata, autoload=True, autoload_with=engine)
	#regions = db.Table('regions', metadata, autoload=True, autoload_with=engine)
	#products = db.Table('products', metadata, autoload=True, autoload_with=engine)
	#stats = db.Table('stats', metadata, autoload=True, autoload_with=engine)
	#product_status = db.Table('product_status',metadata,autoload=True,autoload_with=engine)


	# override init inheritance; MODIS dates are different
	def __init__(self,file_path:str):
		if self.noCred:
			raise NoCredentialsError("Database credentials not found. Image objects cannot be instantialized. Use 'glamconfigure' on command line to set archive credentials.")
		self.type = "image"
		if not os.path.exists(file_path):
			raise BadInputError(f"File {file_path} not found")
		self.path = file_path
		self.product = os.path.basename(file_path).split(".")[0]
		if self.product not in octvi.supported_products:
			raise BadInputError(f"Product type '{self.product}' not recognized")
		self.collection = '006'
		self.year = os.path.basename(file_path).split(".")[1]
		self.doy = os.path.basename(file_path).split(".")[2]
		self.date = datetime.strptime(f"{self.year}-{self.doy}","%Y-%j").strftime("%Y-%m-%d")
		self.admins = admins
		self.crops = crops
		#print(os.path.join(os.path.dirname(os.path.abspath(__file__)),"statscode","Masks",f"{self.product[:2]}*.{crop}.tif"))
		self.cropMaskFiles = {crop:glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)),"statscode","Masks",f"M*D*.{crop}.tif"))[0] for crop in self.crops if crop != "nomask"}
		self.cropMaskFiles['nomask'] = None
		self.adminFiles = {level:glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)),"statscode","Regions",f"M*D*.{level}.tif"))[0] for level in self.admins}

	# override repr inheritance, correct object type
	def __repr__(self):
		return f"<Instance of ModisImage, product:{self.product}, date:{self.date}, collection:{self.collection}, type:{self.type}>"

	def ingest(self) -> bool:
		"""
		Uploads the file at self.path to the aws s3 bucket, and inserts the corresponding base file name into the database
		Returns True on success and False on failure
		"""

		log.debug("-defining variables")
		file_name = os.path.basename(self.path) # extracts directory of image file
		s3_bucket = 'glam-tc-data/rasters/' # name of s3 bucket
		# mysql credentials
		try:
			mysql_user = os.environ['glam_mysql_user']
			mysql_pass = os.environ['glam_mysql_pass']
			mysql_db = 'modis_dev'
		except KeyError:
			raise NoCredentialsError("Database credentials not found. Use 'glamconfigure' on command line to set archive credentials.")

		mysql_db = 'modis_dev'
		rds_endpoint = 'glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com'
		mysql_path = 'mysql://'+mysql_user+':'+mysql_pass+'@'+rds_endpoint+'/'+mysql_db # full path to mysql database

		## update database
		log.debug("-adding file to database")
		driver = tc.get_driver(mysql_path)
		key_names = ('product', 'year', 'day','collection','type')

		# inserting file into database
		keys = {"product":self.product,'collection':self.collection,"year":self.year,"day":self.doy,"type":self.type}
		log.debug(keys)
		s3_path = 's3://'+s3_bucket+file_name
		try:
			driver.insert(keys=keys, filepath=self.path, override_path=f'{s3_path}',ndvi=True)
		except Exception as e:
			log.error(e)
			return False

		## upload file to s3 bucket
		log.debug("-uploading file to s3 bucket")
		def upload_file_s3(upload_file,bucket) -> bool:
			try:
				s3_client = boto3.client('s3',
					aws_access_key_id=os.environ['AWS_accessKeyId'],
					aws_secret_access_key=os.environ['AWS_secretAccessKey']
					)
			except KeyError:
				raise NoCredentialsError("Amazon Web Services (AWS) credentials not found.\nUse 'aws configure' on the command line.")
			
			b = bucket.split("/")[0]
			k = bucket.split("/")[1]+"/"+os.path.basename(upload_file)
			try:
				response = s3_client.upload_file(Filename=upload_file,Bucket=b,Key=k)
			except ClientError as e:
				log.error(e)
				return False
			return True
		u = upload_file_s3(self.path,s3_bucket)

		## on success, update database to match
		if u and (self.type == 'image'):
			updateSql = f"UPDATE product_status SET processed = True WHERE product = '{self.product}' AND date = '{self.date}';"
			with self.engine.begin() as connection:
				try:
					x = connection.execute(updateSql)
					if x.rowcount == 0:
						connection.execute(f"INSERT INTO product_status (product, date, downloaded, processed, completed, statGen) VALUES ('{self.product}','{self.date}',True,True,False,False);")
				except db.exc.OperationalError:
					return False

		## return True if everything succeeded, or False otherwise
		return u

	# override uploadStats() to use windowed read
	def uploadStats(self,stats_tables=None,admin_level="ALL",crop_level="ALL",override_brazil_limit=False) -> None:
		"""
		Calculates and uploads all statistics for the given data file to the database

		Description
		-----------
		For each crop x admin combination, a pandas dataframe of statistics by region is created
		These combinations are then paired up with the corresponding stats table name, as found in stats_tables
		For each table:
			If the table does not exist (table.exists==False) then it is created
			The dataframe is uploaded to the table
		If all stats are successfully uploaded, the function updates product_status.statGen to True

		***

		Parameters
		----------
		stats_tables:dict
			Nested dictionary of stats table IDs. Create with getStatsTables()
		admin_level:str
			One of "ALL", "GAUL", or "BRAZIL". Defines which admin levels will
			have statistics run. Allows for running only certain combinations.
		crop_level:str
			One of "ALL", "NOMASK", "CROPMONITOR", or "BRAZIL". Defines which crop masks will
			have statistics run. Allows for running only certain combinations.
		override_brazil_limit:bool
			If False (default), only run brazil crops for brazil regions. If True, runs brazil
			crops for ALL regions.
		"""

		def zonal_stats(image_path:str, crop_mask_path:str, admin_path:str) -> 'pandas.DataFrame':
			"""
			Generate pandas dataframe of statistics for a given combination of image x mask x admins
			Returns a pandas dataframe of statistics by admin ID: arable pixels, clear pixels, percent clear, and mean of image raster within clear arable pixels.
			...

			Parameters
			----------
			image_path:str
				file path to a modis raster image
			crop_mask_path:str
				file path to a crop mask (binary raster image of same resolution as data)
			admin_path:str
				administrative region path (categorical raster of same resolution)
			"""
			#Process in tile sized batches
			import gdal
			GA_ReadOnly = 0
			
			xBSize = 512
			yBSize = 512
			stats = []
			flatarrays = {}

			log.debug(f"running: {admin_path}, {crop_mask_path}, {image_path}")
			### Open the admin unit file first, should be a tif file
			### No admin unit = 0 value
			### Everything is pixel based and preprocessed, no need to worry about the geotransforms
			adminds = gdal.Open(admin_path, GA_ReadOnly)
			#assert adminds
			adminbandhandle = adminds.GetRasterBand(1)
			adminnodata = adminbandhandle.GetNoDataValue()

			### Open the crop mask file, should also be a tif file
			### No crop = 0 (no data), 1 = crop
			if crop_mask_path:
				cmds = gdal.Open(crop_mask_path, GA_ReadOnly)
				#assert cmds
				cmbandhandle = cmds.GetRasterBand(1)
				cmnodata = cmbandhandle.GetNoDataValue()
			else:
				cmds = None
				cmbandhandle=None
				cmnodata= 0

			### Open the ndvi file, should be a tif file
			ndvids = gdal.Open(image_path, GA_ReadOnly)
			#assert ndvids
			### The name of the NDVI band for C6 data:
			ndvibandhandle = ndvids.GetRasterBand(1)
			ndvinodata = ndvibandhandle.GetNoDataValue()
			rows = ndvids.RasterYSize
			cols = ndvids.RasterXSize

			blockN = 0
			for i in range(0, rows, yBSize):
				if ((i + yBSize) < rows):
					numRows = yBSize
				else:
					numRows = rows - i
				for j in range(0, cols, xBSize):
					if ((j + xBSize) < cols):
						numCols = xBSize
					else:
						numCols = cols - j
					# Process each block here
					blockN += 1
					#log.debug(f"Block {blockN}")
					adminband = adminbandhandle.ReadAsArray(j, i, numCols, numRows)
					try:
						cmband = cmbandhandle.ReadAsArray(j, i, numCols, numRows)
					# if no crop mask, just make an array of all 1s
					except:
						cmband = np.full((numRows,numCols),1)
					ndviband = ndvibandhandle.ReadAsArray(j, i, numCols, numRows)

					# Loop over the unique values in the admin layer
					uniqueadmins = np.unique(adminband[adminband != adminnodata])

					# Loop through admin units, skip 0
					for adm in uniqueadmins:
						thisadm = str(adm)
						# Mask the source data array with our current feature
						# we also mask out nodata values explictly
						statcountarable = int((adminband[(adminband == adm) & (cmband != cmnodata)]).size)
						if statcountarable == 0:
							continue
						masked = np.array(ndviband[(ndviband != ndvinodata) & (cmband != cmnodata) & (adminband == adm)], dtype='int64')
						statcount = masked.size
						if thisadm not in flatarrays:
							flatarrays[thisadm] = {
								'values': (masked.mean() if (statcount > 0) else 0),
								'count': statcount,
								'countarable' : statcountarable
							}
						else:
							updatedcount = flatarrays[thisadm]['count'] + statcount
							if updatedcount > 0:
								if np.isnan(np.sum(flatarrays[thisadm]['values'])):
									flatarrays[thisadm]['values'] = masked.sum() / updatedcount
								else:
									flatarrays[thisadm]['values'] = ((flatarrays[thisadm]['values'] * flatarrays[thisadm]['count']) + masked.sum()) / updatedcount
								flatarrays[thisadm]['count'] = updatedcount
							else:
								flatarrays[thisadm]['count'] = 0
								flatarrays[thisadm]['values'] = 0
							flatarrays[thisadm]['countarable'] += statcountarable

			alladms = list(flatarrays.keys())
			for finaladm in alladms:
				values = flatarrays[finaladm]['values']
				count = flatarrays[finaladm]['count']
				arable_count = flatarrays[finaladm]['countarable']
				try:
					feature_stats = {
						'value': values,
						'count': count,
						'arable': arable_count,
						'pct': float(count) / float(arable_count) * 100,
						'admin': finaladm
					}
					stats.append(feature_stats)
				except ValueError: #Array size is zero, do nothing
					warnings.warn("No pixels found for admin zone: {}".format(finaladm))
			try:
				header = list(stats[0].keys())
				header.sort()
			except IndexError: #Ag mask doesn't overlap admin mask (e.g. Brazil x SpringWheat)
				log.warning(f"No mask-region overlap for {crop_mask_path} and {admin_path}")
				return None

			sortedData = {}
			for v in header:
				sortedData[v] = []
			for stat in stats:
				for k in stat.keys():
					sortedData[k].append(stat[k])
			return pd.DataFrame(sortedData)

		def create_stats_table(table_name:str,df:'pandas.DataFrame') -> None:
			newCol_val = f"val.{self.doy}"
			newCol_pct = f"pct.{self.doy}"
			df=df.rename(columns={"value":newCol_val,"pct":newCol_pct})
			df_subset = df[["admin","arable",newCol_val,newCol_pct]]
			log.info(f"--new table: {table_name}")
			with self.engine.begin() as connection:
				connection.execute(f"CREATE TABLE {table_name} (`admin` int, `arable` int, `{newCol_val}` float(2), `{newCol_pct}` float(2));") # create empty table with correct columns for one day's worth of data
				df_subset.to_sql(f"{table_name}",self.engine,if_exists='append',index=False) # add data as rows to the newly-created table
				connection.execute(f"CREATE INDEX index_{table_name} on {table_name}(admin);") # create index on admin column for faster lookups

		def append_to_stats_table(table_name:str,df:'pandas.DataFrame') -> None:
			"""
			Given a stats table name and a pandas dataframe, first checks to see whether the desired columns exist; if not, creates them and fills them with correct stats values
			...

			Parameters
			----------
			table_name:str
				name of the stats table, in format "stats_{stats_id}"
			df:pandas.DataFrame
				pandas dataframe of statistics for the image x mask x admin combination in question
			"""
			newCol_val = f"val.{self.doy}"
			newCol_pct = f"pct.{self.doy}"
			with self.engine.begin() as connection:
				try:
					connection.execute(f"SELECT `{newCol_val}` FROM {table_name}") # try to select the desired columns
					log.debug(f"-column {newCol_val} already exists in table {table_name}")
				except db.exc.InternalError: # if the column does not exist, the attempt to select it will throw an error
					# alter the table to add the desired columns
					log.debug(f"Appending new column {newCol_val} to table {table_name}")
					aSql = f"ALTER TABLE {table_name} ADD `{newCol_val}` float(2)"
					connection.execute(aSql)
				try:
					connection.execute(f"SELECT `{newCol_pct}` FROM {table_name}") # as above, but for newcol_pct
					log.debug(f"-column {newCol_pct} already exists in table {table_name}")
				except db.exc.InternalError:
					log.debug(f"Appending new column {newCol_pct} to table {table_name}")
					aSql = f"ALTER TABLE {table_name} ADD `{newCol_pct}` float(2)"
					connection.execute(aSql)
				for index, row in df.iterrows():
					uSql= f"UPDATE {table_name} SET `{newCol_val}`={row['value']} WHERE admin = {row['admin']}"
					uRows = connection.execute(uSql)
					uSql= f"UPDATE {table_name} SET `{newCol_pct}`={row['pct']} WHERE admin = {row['admin']}"
					connection.execute(uSql)
					if uRows.rowcount == 0: # admin does not yet exist in table
						# append new row
						log.debug(f"Inserting new row for admin {row['admin']} into table {table_name}")
						iSql = f"INSERT INTO {table_name} (admin, arable, `{newCol_val}`, `{newCol_pct}`) VALUES ({row['admin']}, {row['arable']}, {row['value']}, {row['pct']});"
						connection.execute(iSql)

		try:
			if stats_tables is None:
				stats_tables = self.getStatsTables()
			#log.info(zonal_stats(self.path,self.cropMaskFiles['winterwheat'],self.adminFiles['gaul1'])['value'])
			#return 0
			for crop in stats_tables.keys():
				if crop_level == "NOMASK" and crop != 'nomask':
					continue
				if crop_level == "BRAZIL" and crop not in crops_brazil:
					continue
				if crop_level == "CROPMONITOR" and crop not in crops_cropmonitor:
					continue
				for admin in stats_tables[crop].keys():
					if admin_level == "BRAZIL" and admin not in admins_brazil:
						continue
					if admin_level == "GAUL" and admin not in admins_gaul:
						continue
					if not override_brazil_limit:
						if crop in crops_brazil and admin not in admins_brazil:
							continue
					statsTable = stats_tables[crop][admin] # extract correct StatsTable object, with fields .name:str and .exists:bool
					statsDataFrame = zonal_stats(self.path,self.cropMaskFiles[crop],self.adminFiles[admin]) # generate data frame of statistics
					#return statsDataFrame
					if statsDataFrame is not None: # check if zonal_stats returned a dataframe or None
						if statsTable.exists: # if the stats table already exists, append the new columns to it
							append_to_stats_table(statsTable.name,statsDataFrame)
						else: # if the stats table does not exist, create it with the stats information already in
							create_stats_table(statsTable.name,statsDataFrame)
					else: # if zonal_stats returned None, that means the combination of crop/admin is invalid (for example, there is no overlap beetween spring wheat and the Brazil masks)
						continue
		except db.exc.OperationalError: # sometimes, the database just randomly conks out. No idea why. This restarts the attempt as many times as needed. Watch out for rogue loops.
			log.warning("WARNING: Lost connection to database. Trying again.")
			self.uploadStats(stats_tables)
			
		## update product_status if all stats uploaded
		if admin_level == "ALL" and crop_level == "ALL":
			with self.engine.begin() as connection:
				updateSql = f"UPDATE product_status SET statGen = True WHERE product = '{self.product}' AND date = '{self.date}';"
				x = connection.execute(updateSql)
				if x.rowcount == 0:
					connection.execute(f"INSERT INTO product_status (product, date, downloaded, processed, completed, statGen) VALUES ('{self.product}','{self.date}',True,False,False,True);")


## define functions

def getImageType(in_path:str) -> Image:
	"""
	Given path to downloaded file, returns
	either ModisImage or AncillaryImage,
	depending on the file type
	"""
	p = os.path.basename(in_path).split(".")[0]
	if p in ancillary_products:
		return AncillaryImage
	elif p in octvi.supported_products:
		return ModisImage
	else:
		raise BadInputError(f"Image type '{p}' not recognized.")

# erases all records of a file from the s3 bucket and all databases -- USE ONLY AS A LAST RESORT
def purge(product, date, auth_key) -> bool:
	"""
	This function expunges a given product-date combination from existence, as if it never was. The files will be removed, all records will be deleted, and life will continue as usual.
	This function 'un-persons' the file.
	It is intended to be used when chirps-prelim data is replaced by the final chirps data. In any other case, the user is exhorted to hesitate before calling this function of death.

	...

	Parameters
	----------
	product: str
		The string representation of the product type to be purged
	date: str
		The date of the file to be purged, string formatted as "%Y-%m-%d"
	auth_key: str
		Password authorizing bearer to delete files. This power is not to be used lightly.

	"""
	m = hashlib.sha256()
	m.update(auth_key.encode('ASCII'))
	auth_hash = m.digest()

	if auth_hash != b'\x7f\\\x04u\xa7\xbf\xa4R\xc7\xc9\xd7{\xbaw\x7f\x80;\x00~\x9d#\xa2\x81M:\xc1\xe2B?\xb9F{':
		log.error(f"Unauthorized with key: '{auth_key}'")
		return None

	# mysql credentials
	try:
		mysql_user = os.environ['glam_mysql_user']
		mysql_pass = os.environ['glam_mysql_pass']
		mysql_db = 'modis_dev'
	except KeyError:
		raise NoCredentialsError("Database credentials not found. Use 'glamconfigure' on command line to set archive credentials.")

	else:
		# setup
		engine = db.create_engine(f'mysql+pymysql://{mysql_user}:{mysql_pass}@glam-tc-dev.c1khdx2rzffa.us-east-1.rds.amazonaws.com/{mysql_db}')

		# pull file to disk to get information
		downloader = Downloader()
		try:
			local_file = downloader.pullFromS3(product, date, os.path.dirname(__file__))[0]
		except IndexError:
			# there is no such file in the database
			log.warning(f"Failed to delete {product} {date}")
			return None
		img = Image(local_file)
		stats_tables = img.getStatsTables()

		# collect all associated stats table names/ids
		stats_names = []
		for crop in stats_tables.keys():
			for region in stats_tables[crop].keys():
				st = stats_tables[crop][region]
				if st.exists:
					stats_names.append(st.name)
		#stats_ids = [name.split("_")[1] for name in stats_names]

		# delete stats tables columns
		colnames = [f"{p}.{img.doy}" for p in ('val','pct')]
		for name in stats_names:
			with engine.begin() as connection:
				for col in colnames:
					try:
						#delete column
						connection.execute(f"ALTER TABLE {name} DROP COLUMN {col};")
					except db.exc.InternalError:
						log.warning(f"In attempting to remove stats for {img.product} {img.date}: Column '{col}' does not exist in table '{name}'")

		# delete record in `product status`
		with engine.begin() as connection:
			connection.execute(f"DELETE FROM product_status WHERE product = '{product}' AND date = '{date}';")

		# delete record in `datasets`
		year = datetime.strptime(date,"%Y-%m-%d").strftime("%Y")
		doy = datetime.strptime(date,"%Y-%m-%d").strftime("%j")
		with engine.begin() as connection:
			connection.execute(f"DELETE FROM datasets WHERE product = '{product}' AND year = {year} AND day = {doy};")

		# remove file from S3
		s3_resource = boto3.resource('s3',
			aws_access_key_id=os.environ['AWS_accessKeyId'],
			aws_secret_access_key=os.environ['AWS_secretAccessKey']
			)
		s3_bucket = 'glam-tc-data/rasters/' # name of s3 bucket
		b = s3_bucket.split("/")[0]
		k = s3_bucket.split("/")[1]+"/"+os.path.basename(local_file)
		s3_obj = s3_resource.Object(b,k)
		s3_obj.delete()

		# delete local file on disk
		os.remove(local_file)
		return True

# full process of finding missing files, downloading them from source, uploading them to S3+database, and generating statistics
def updateGlamData():
	"""
	This function finds all missing GLAM data (currently ancillary files only) and attempts to fully ingest and process each one
	"""
	## create necessary objects
	downloader = Downloader() # downloader object
	missingFiles = ToDoList() # collect missing dates for each file type
	missingFiles.filterUnavailable() # pare down to only available files
	## iterate over ToDoList object
	for f in missingFiles:
		#product = f[0]
		#date = f[1]
		log.info("{0} {1}".format(*f))
		try:
			if f[0] not in ancillary_products:
				continue
			if not downloader.isAvailable(*f): # this should never happen
				raise UnavailableError("No file detected")
			paths = downloader.pullFromSource(*f,f"\\\\webtopus.iluci.org\\c$\\data\\Dan\\{f[0]}_archive")
			# check that at least one file was downloaded
			if len(paths) <1:
				raise UnavailableError("No file detected")
			log.debug("-downloaded")
			# iterate over file paths
			for p in paths:
				#log.debug(p)
				image = getImageType(p)(p) # create correct of ModisImage or AncillaryImage object
				#if image.product == 'chirps':
				#	log.debug("-purging corresponding chirps-prelim product")
				#	purge('chirps-prelim',image.date,None)
				image.setStatus('downloaded',True)
				log.debug(f"-collection: {image.collection}")
				ingest = image.ingest()
				if ingest:
					image.setStatus('processed',True)
					log.debug("--ingested")
				stats = image.uploadStats()
				if stats:
					image.setStatus('statGen',True)
					log.debug("--stats generated")
				#os.remove(p) # once we fully move to aws, we'll download 1 file at a time and remove them when no longer needed
				#log.debug("--file removed")
		# again, this should never happen
		except UnavailableError:
			log.info("(No file available)")
		except:
			log.error("FAILED")
			continue

# main function
def main():
	updateGlamData()
	log.info(f"{os.path.basename(__file__)} finished {datetime.today()}")


########################################

if __name__ == "__main__":
	main()