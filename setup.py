import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "pyrosql",
    version = "0.1.0",
    author = "Stelios Voutsinas",
    author_email = "stv@roe.ac.uk",
    description = ("A sql client for accessing and querying databases using pyodbc"),
    license = "BSD",
    keywords = "pyrosql python sqlclient mysql",
    packages=['pyrosql', 'pyrosql.pyrosql'],
    tests_require=['selenium'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Topic :: Utilities",
        "License :: GPL License",
    ],
    include_package_data = True,  
    install_requires=[
        'numpy>=1.4.0',
        'astropy>=0.4.1',
        'pyodbc'
    ]
)

