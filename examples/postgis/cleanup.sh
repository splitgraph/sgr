#!/bin/bash -ex

# Quick cleanup script that deletes added/built data
rm nyc.png -f
rm vote_map.png -f

sgr rm -y vote_map
sgr rm -y splitgraph/election-geodata
sgr cleanup

sgr status
