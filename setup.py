# ========================================================
# Licensed Materials - Property of IBM
# 5737-I32
#
# (C) Copyright IBM Corp. 2019
#
# US Government Users Restricted Rights - Use, duplication, or
# disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# ========================================================
"""This module contains the setup for creating a Python Wheel."""

from setuptools import find_packages, setup


with open('requirements.txt', 'rt') as r:
    DEPENDENCIES = r.read().splitlines()

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='ibm_spectrum_discover_application_sdk',
    version='0.0.7',
    description='IBM Spectrum Discover Application SDK',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/IBM/Spectrum_Discover_Application_SDK",
    author='Drew Olson',
    author_email='drolson@us.ibm.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=DEPENDENCIES,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
