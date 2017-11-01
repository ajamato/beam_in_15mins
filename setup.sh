#!/bin/bash

# Install tools
pip install --upgrade virtualenv
pip install --upgrade setuptools

# Create and start virtualenv
DATETIME=`date +%Y%m%d%H%M%S` 
DIR="$HOME/beam_in_15_virt$DATETIME"
mkdir "$DIR"
virtualenv "$DIR"
. $DIR/bin/activate

# Install apache-beam into the virtual environment
pip install apache-beam


