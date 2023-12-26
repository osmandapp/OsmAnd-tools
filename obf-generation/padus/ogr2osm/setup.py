#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools

exec(open("ogr2osm/version.py").read())

with open('README.md', 'r', encoding='utf-8') as fh:
    README = fh.read()

setuptools.setup(
    name="ogr2osm", #__program__,
    version=__version__,
    license=__license__,
    author=__author__,
    author_email="ogr2osm.pypi@derickx.be",
    description="A tool for converting ogr-readable files like shapefiles into .osm or .pbf data",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/roelderickx/ogr2osm",
    packages=setuptools.find_packages(),
    python_requires='>=3.7',
    install_requires=['lxml>=4.3.0', 'GDAL>=3.0.0'],
    entry_points={
        'console_scripts': ['ogr2osm = ogr2osm.ogr2osm:main']
    },
    classifiers=[
        'Environment :: Console',
        'Topic :: Scientific/Engineering :: GIS',
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
