#!/bin/bash

# A script to generate source files from the analysis notebooks, for diffing. 
# pip install --user jupyter

NB_DIR=analyses
SRC_DIR=$NB_DIR/src
mkdir -p $SRC_DIR

jupyter nbconvert --to python $NB_DIR/*.ipynb
mv $(find $NB_DIR/*.py) $SRC_DIR