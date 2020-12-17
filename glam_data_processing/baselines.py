#! /usr/bin/env python

"""

"""

# set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)


from .util import *
from octvi import supported_products
import gdal

BASELINE_DIR = os.path.join(RASTER_DIR,'baselines')
PRODUCT_DIR = os.path.join(RASTER_DIR,'products')


def updateBaselines(product, date) -> dict:
    """Updates anomaly baselines

    ***

    Parameters
    ----------
    product:str
    date:str

    Returns
    -------
    Dictionary with the following key/value pairs:
        product:str
            Product name
        paths:tuple
            Tuple of filepaths of the anomalybaseline
            files that were updated
    """

    return {'product':None,'paths':()}


def _getMatchingBaselineDate(product,date:datetime) -> str:
    """Returns baseline date that corresponds to given image date"""
    # for ndvi and merra-2 products, we can just use DOY
    if (product in supported_products) or ("merra-2" in product):
        return date.strftime("%j")
    # for chirps we return month-day; as all chirps datasets
    # fall on the same mm-dd pattern every year
    elif product == "chirps":
        return date.strftime("%m-%d")
    # swi is the trickiest, as we must find which default
    # baseline period is closest to the input date
    elif product == "swi":
        doy = int(date.strftime("%j"))
        for baseline_doy in _getSwiBaselinePeriods():
            if _isClosestSwiDoy(doy, baseline_doy):
                return str(baseline_doy).zfill(3)
    else:
        raise BadInputError(f"Product '{product}' not recognized")


def _getSwiBaselinePeriods() -> list:
    """Returns swi baseline periods"""
    # every 5th DOY
    return [i for i in range(1,366,5)]


def _isClosestSwiDoy(doy_1:int, doy_2:int) -> bool:
    return min((doy_1-doy_2) % 365, (doy_2 - doy_1) % 365) < 3


def _listFiles(product,date:datetime) -> list:
    """Returns a list of matching files, one from each year"""
    allFiles = glob.glob(os.path.join(PRODUCT_DIR,product,"*.tif"))
    output_files = []
    years_considered = []
    # COLLECT FILES BY PRODUCT
    for f in allFiles:
        meta = getMetadata(f)
        # we only go back 10 years; skip file if gap is larger
        if date.strftime("%Y") - meta['year'] > 10:
            continue
        years_considered.append(int(meta['year']))
        years_considered = list(set(years_considered)) # remove duplicate years
        # for ndvi and merra-2 products, we can just use DOY
        if (product in supported_products) or ("merra-2" in product):
            if meta['doy'] == date.strftime("%j"):
                output_files.append(f)
        # for chirps we rely on month and day
        elif product == "chirps":
            if date.strftime("%m-%d") == meta['date_obj'].strftime("%m-%d"):
                output_files.append(f)
        # our old enemy, swi... check if the doy is within 3 days
        elif product == "swi":
            if _isClosestSwiDoy(date.strftime("%j"),meta['doy']):
                output_files.append(f)
        else:
            raise BadInputError(f"Product '{product}' not recognized")
    # check that we have exactly one file from each year
    n_years_considered = ((max(years_considered) - min(years_considered)) + 1)
    if len(output_files) != n_years_considered:
        log.warning(f"{n_years_considered} years considered but {len(output_files)} files collected!")
    # return the output list
    return output_files


def _mp_worker(args) -> tuple:
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
    args:tuple
        targetwindow:rasterio window
        input_paths:list
            Ordered list of filepaths

    """
    targetwindow, input_paths,dtype = *args

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

        # if yearscounter == len(input_paths):
        # 	# Calculate the full time series mean and write it
        # 	data_full = np.array(valuestore)
        # 	mean_full_calc = np.ma.average(data_full, axis=0, weights=((data_full >= -1000) * (data_full <= 10000)))
        # 	mean_full_calc[mean_full_calc.mask==True] = -3000
        # 	outputstore['mean_full'] = mean_full_calc.astype(dtype)
        # 	del mean_full_calc

        # 	# Calculate the full time series median and write it
        # 	median_full_masked = np.ma.masked_outside(data_full, -1000, 10000)
        # 	median_full_calc = np.ma.median(median_full_masked, axis=0)
        # 	median_full_calc[median_full_calc.mask==True] = -3000
        # 	outputstore['median_full'] = median_full_calc.astype(dtype)
        # 	del data_full, median_full_masked, median_full_calc
        inputhandle.close()
    return(targetwindow, outputstore)
