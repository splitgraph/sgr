#!/bin/bash -ex

jupyter nbconvert --ExecutePreprocessor.kernel_name=python3 --to notebook --execute --inplace -- vote_map.ipynb