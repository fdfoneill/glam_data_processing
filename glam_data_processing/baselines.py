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
import gdal, glob

BASELINE_DIR = os.path.join(RASTER_DIR,'baselines')
PRODUCT_DIR = os.path.join(RASTER_DIR,'products')


def updateBaselines(product, date:datetime, n_workers=20, block_scale_factor= 1, time=False) -> dict:
    """Updates anomaly baselines

    ***

    Parameters
    ----------
    product:str
    date:datetime
    n_workers:int
    block_scale_factor:int
    time:bool

    Returns
    -------
    Dictionary with the following key/value pairs:
        product:str
            Product name
        paths:tuple
            Tuple of filepaths of the anomalybaseline
            files that were updated
    """

    startTime = datetime.now()

    # create dict of anomaly baseline folders for each baseline type
    baseline_locations = {anomaly_type:os.path.join(BASELINE_DIR,product,anomaly_type) for anomaly_type in ["mean_5year","median_5year",'mean_10year','median_10year']}
    # get list of input data files
    input_paths = _listFiles(product,date)
    # check to make sure we got at least 10
    if len(input_paths) < 10:
        raise OperationalError(f"Only {len(input_paths)} input image paths found")
    # get raster metadata and dimensions
    with rasterio.open(input_paths[0]) as tempmeta:
        metaprofile = tempmeta.profile
        width = tempmeta.width
        height = tempmeta.height
    # add BIGTIFF where necessary
    if product in supported_products:
        metaprofile['BIGTIFF'] = 'YES'

    # set output filenames
    output_date = _getMatchingBaselineDate(product,date)
    mean_5yr_name = os.path.join(baseline_locations["mean_5year"], f"{product}.{output_date}.anomaly_mean_5year.tif")
    median_5yr_name = os.path.join(baseline_locations["median_5year"], f"{product}.{output_date}.anomaly_median_5year.tif")
    mean_10yr_name = os.path.join(baseline_locations["mean_10year"], f"{product}.{output_date}.anomaly_mean_10year.tif")
    median_10yr_name = os.path.join(baseline_locations["median_10year"],f"{product}.{output_date}.anomaly_median_10year.tif")

    # open output handles
    log.debug("Opening handles")
    mean_5yr_handle = rasterio.open(mean_5yr_name, 'w', **metaprofile)
    median_5yr_handle = rasterio.open(median_5yr_name, 'w', **metaprofile)
    mean_10yr_handle = rasterio.open(mean_10yr_name, 'w', **metaprofile)
    median_10yr_handle = rasterio.open(median_10yr_name, 'w', **metaprofile)

    # set block size and get windows
    blocksize = metaprofile['blockxsize'] * int(block_scale_factor)
    windows = getWindows(width,height,blocksize)

    # do multiprocessing
    log.info(f"Processing ({sub_product} {new_image.date}) | {n_workers} parallel processes")
    parallelStartTime = datetime.now()
    p = multiprocessing.Pool(n_workers)

    for win, values in p.imap(_mp_worker, windows):
        mean_5yr_handle.write(values['mean_5year'], window=win, indexes=1)
        median_5yr_handle.write(values['median_5year'], window=win, indexes=1)
        mean_10yr_handle.write(values['mean_10year'], window=win, indexes=1)
        median_10yr_handle.write(values['median_10year'], window=win, indexes=1)

    ## close pool
    p.close()
    p.join()

    ## close handles
    mean_5yr_handle.close()
    median_5yr_handle.close()
    mean_10yr_handle.close()
    median_10yr_handle.close()

    # cloud-optimize new anomalies
    log.debug("Converting baselines to cloud-optimized geotiffs and ingesting to S3")
    cogStartTime = datetime.now()

    # cloud-optimize outputs
    output_paths = (mean_5yr_name, median_5yr_name,mean_10yr_name, median_10yr_name)

    p = multiprocessing.Pool(len(output_paths))
    p.imap(cloud_optimize_inPlace,output_paths)

    ## close pool
    p.close()
    p.join()

    # if time==True, log total time for anomaly generation
    endTime = datetime.now()
    if time:
        log.info(f"Finished in {endTime-startTime}")

    # return dict
    return {'product':product, 'paths':output_paths}



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
        periods = _getSwiBaselinePeriods()
        while doy not in periods:
            doy = (doy - 1) % 365
        return str(doy).zfill(3)
    else:
        raise BadInputError(f"Product '{product}' not recognized")


def _getSwiBaselinePeriods() -> list:
    """Returns swi baseline periods"""
    # every 5th DOY
    return [i for i in range(1,366,5)]


def _listFiles(product,date:datetime,n_years_to_consider = 10) -> list:
    """Returns a list of matching files, one from each year
    Output is sorted by year; latest first
    """
    allFiles = glob.glob(os.path.join(PRODUCT_DIR,product,"*.tif"))
    output_files = []
    years_considered = []
    # COLLECT FILES BY PRODUCT
    for f in allFiles:
        meta = getMetadata(f)
        # we only go back 10 years; skip file if gap is larger
        if int(date.strftime("%Y")) - int(meta['year']) > (n_years_to_consider - 1):
            continue
        # also skip extra doys from leap years
        if int(meta['doy']) > 365:
            continue
        years_considered.append(int(meta['year']))
        years_considered = list(set(years_considered)) # remove duplicate years
        # find matching baseline date for input and current file; add if they match
        baseline_date = _getMatchingBaselineDate(product,date)
        if _getMatchingBaselineDate(product,meta['date_obj']) == baseline_date:
            output_files.append(f)
    # check that we have exactly one file from each year
    n_years_considered = ((int(date.strftime("%Y")) - min(years_considered)) + 1)
    if len(output_files) != n_years_considered:
        log.warning(f"{n_years_considered} years considered but {len(output_files)} files collected!")
    # sort list by file year; latest first
    output_files = sorted(output_files, key=lambda filepath: int(getMetadata(filepath)['year']), reverse=True)
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
    targetwindow, input_paths,dtype = args

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
            # after calculating the 10-year baseline, we can stop opening files
            inputhandle.close()
            break

        # don't forget to close the inputhandle!
        inputhandle.close()
    return(targetwindow, outputstore)
