#!/bin/bash

# Run from inside the virtual env
pip install apache-beam

# Run the program
#python -m apache_beam.examples.wordcount --input data/sample_words.txt --output counts
python pipeline.py