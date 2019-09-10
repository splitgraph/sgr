#!/bin/bash

# Reruns all notebooks in all subdirectories. Should be run inside of an env with SG + required libraries
# (e.g. sklearn/jupyter etc) installed.

find . -not -path '*/\.*' -type f -name '*.ipynb' \
  -exec jupyter nbconvert --ExecutePreprocessor.kernel_name=python3 --to notebook --execute --inplace -- {} \;
