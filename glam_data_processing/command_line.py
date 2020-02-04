import os

def setCredentials():
	## data archive
	cont = input("Set credentials for the MERRA-2 archive and Copernicus online? [Y/N]\n")
	if cont.lower() in ("y","yes"):
		# get input
		merraUsername = input("Merra-2 archive username:\n")
		merraPassword = input("Merra-2 archive password:\n")
		swiUsername = input("Copernicus online username:\n")
		swiPassword = input("Copernicus online password:\n")
		# use input to set environent variables
		os.environ['merrausername'] = merraUsername
		os.environ['merrapassword'] = merraPassword
		os.environ['swiusername'] = swiUsername
		os.environ['swipassword'] = swiPassword
	elif cont.lower() in ("n","no"):
		pass
	else:
		print("Error: Input not recognized. Please select one of: [Y/N]")
		setCredentials()
	## database
	cont = input("Set credentials for database? [Y/N]\n")
	if cont.lower() in ("y","yes"):
		# get input
		mysql_user = input("MySQL username:\n")
		mysql_pass = input("MySQL password:\n")
		# use input to set environent variables
		os.environ['glam_mysql_user'] = mysql_user
		os.environ['glam_mysql_pass'] = mysql_pass
	elif cont.lower() in ("n","no"):
		pass
	else:
		print("Error: Input not recognized. Please select one of: [Y/N]")
		setCredentials()