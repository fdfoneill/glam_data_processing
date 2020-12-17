#! /usr/bin/env python

"""

"""

# set up logging
import logging, os
from datetime import datetime, timedelta
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
#logging.basicConfig(level="DEBUG")
log = logging.getLogger(__name__)

import gdal

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


def _getMatchingBaselineDate(product,date) -> str:
    """Returns baseline date that corresponds to given image date"""
    return ""
