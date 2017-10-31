#!/bin/bash

# Verify the correct versions? Or assume cloud shell has them?
python --version
pip --version

# TODO should we make it easier to install pip and python?
# sudo easy_install pip
# Need to know which OS and which tool to try.

# Install tools
echo "START installing"
pip install --upgrade virtualenv
pip install --upgrade setuptools
echo "DONE installing"

# TODO user --user? OSX has this issue installing.
# see https://github.com/ipython/ipython/issues/9523

# Create and start virtualenv
DATETIME=`date +%Y%m%d%H%M%S` 
DIR="$HOME/beam_in_15_virt$DATETIME"
mkdir "$DIR"
echo "Creating virtualenv $DIR"
virtualenv "$DIR"
echo "Please run\n:"
echo ". $DIR/bin/activate"

# TODO Can we get this to autmatically work? Doesn't seem to stay in virtual env
. "$DIR/bin/activate"

