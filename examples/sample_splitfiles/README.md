# Sample Splitfiles

## Introduction

This directory contains loose sample Splitfiles that can be run against various datasets
 in Splitgraph Cloud.

## Running

You need to be logged into Splitgraph Cloud. You can register for Splitgraph Cloud with `sgr cloud register`.

To run a Splitfile, do

    sgr build [filename] -o [output repository name, optional]

You can also run a Splitfile directly from Github:

    curl -SsL https://raw.githubusercontent.com/splitgraph/splitgraph/master/examples/sample_splitfiles/[SPLITFILE_NAME].splitfile | sgr build - -o [repository_name]
    
For example:

    curl -SsL https://raw.githubusercontent.com/splitgraph/splitgraph/master/examples/sample_splitfiles/county_votes.splitfile | sgr build - -o county_votes

Each Splitfile contains extensive comments about what it does.
