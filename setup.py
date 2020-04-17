# inspired by: https://github.com/pypa/sampleproject/blob/master/setup.py

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()
with open(path.join(here, "requirements.txt")) as f:
    requirements = f.readlines()

setup(
    name="irene_api",
    version="0.1",
    description="Python JSON over HTTP API to Irene.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jjfiv/irene",
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=requirements,
    packages=find_packages(include=("irene/*")),
    package_data={"README.md": ["README.md"]},
)
