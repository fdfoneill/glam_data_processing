#!/usr/bin/env python

import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

def readme():
	with open('README.md') as f:
		return f.read()

setup(name='glam_data_processing',
		version='1.0.0',
		description='Data downloading and handling for GLAM system',
		long_description=readme(),
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
				'glamupdatedata=glam_data_processing.command_line:updateData'
				]
			}
		)