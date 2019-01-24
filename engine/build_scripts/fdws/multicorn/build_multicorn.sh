#!/bin/bash

git clone git://github.com/Kozea/Multicorn.git
ln -s /usr/bin/python3 /usr/bin/python
cd Multicorn && echo "" > preflight-check.sh \
    && make with_python=python3 \
    && make with_python=python3 install
