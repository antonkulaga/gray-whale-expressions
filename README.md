GRAY-WHALE paper supplementary
==============================

This repository contains bioinformatic workflows used in gray whale expressions paper.
The workflows are written with Workflow Description Language http://www.openwdl.org/ 
For each of the workflow example inputs (as json) are give, to reproduce the research - download the samples and change the pathes to correspond to those on your file system.


LAG-s analysis
--------------
Workflows for Longevity Associated Genes analysis  are at workflows/annotate/transcripts
There first indexes are build with vsearch_index.wdl and then aligments to LAGs are done.

Protein predictions for coding genes
-------------------------------------
Coding genes comparisonds are done at at workflows/annotate/transcripts
At first ORFs are extracted and proteins are predicted with Transdecoder
Then Diamond was used to align predicted proteins to Uniref90 clusters 