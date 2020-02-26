## set up logging
import logging, os
logging.basicConfig(level=os.environ.get("LOGLEVEL","INFO"))
log = logging.getLogger("glam_command_line")

import multiprocessing, requests
import glam_data_processing as glam

session = None


def set_global_session():
	global session
	if not session:
		session = requests.Session()