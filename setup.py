#!/usr/bin/env python

import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

def readme():
	with open('README.md') as f:
		return f.read()

exec(open('glam_data_processing/_version.py').read())

setup(name='glam_data_processing',
		version=__version__,
		description='Data downloading and handling for GLAM system',
		long_description=readme(),
		long_description_content_type='text/markdown',
		author="F. Dan O'Neill",
		author_email='fdfoneill@gmail.com',
		license='MIT',
		packages=['glam_data_processing'],
		include_package_data=True,
		# third-party dependencies
		install_requires=[
			'boto3',
			'requests',
			'numpy',
			'octvi',
			'pandas',
			'terracotta',
			'sqlalchemy',
			'gdal'
			],
		# tests
		test_suite='nose.collector',
		tests_require=[
			'nose'
			],
		zip_safe=False,
		# console scripts
		entry_points = {

			'console_scripts': [
				'glamconfigure=glam_data_processing.command_line:setCredentials',
				'glamupdatedata=glam_data_processing.command_line:updateData',
				'glamupdatestats=glam_data_processing.command_line:rectifyStats',
				'glaminfo=glam_data_processing.command_line:getInfo',
				'glamnewstats=glam_data_processing.generate_new_stats:main',
				'glamfillarchive=glam_data_processing.command_line:fillArchive',
				'glamcleanprelim=glam_data_processing.command_line:clean'
				]
			}
		)