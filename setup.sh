#!/bin/bash

# Verify the correct versions? Or assume cloud shell has them?
python --version
pip --version

# TODO should we make it easier to install pip and python?
# sudo easy_install pip
# Need to know which OS and which tool to try.

# Install tools
pip install --upgrade virtualenv
pip install --upgrade setuptools

# TODO user --user? OSX has this issue installing.
# see https://github.com/ipython/ipython/issues/9523

# Create and start virtualenv
DATETIME=`date +%Y%m%d%H%M%S` 
DIR="$HOME/beam_in_15_virt$DATETIME"
mkdir "$DIR"
echo "Creating virtualenv $DIR"
virtualenv "$DIR"
echo "Please activate a virtual environment by running\n:"
echo ". $DIR/bin/activate"

