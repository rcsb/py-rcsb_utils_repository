# RCSB Repository Utilities

## A collection of Python Repository Data Management Utilities

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_utils_repository?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=31&branchName=master)

## Introduction

This module contains a collection of utility classes for accessing and scanning data
stored in various RCSB/PDB repositories.

### Installation

Install via [pip](https://pypi.python.org/pypi/pip).

```bash
pip install rcsb.utils.repository
```

Or, to install from the source, download the library source software from the project repository:

```bash

git clone --recurse-submodules https://github.com/rcsb/py-rcsb_utils_repository.git

```

Optionally, run test suite (Python versions 3.7+) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```bash
python setup.py test

or simply run

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).  To run tests
from the source tree, the package must be installed in editable mode (i.e. -e):

```bash
pip install -e .
```
