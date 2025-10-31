# grammar-extraction-test

This repository contains the [Snakefile](https://snakemake.github.io) to perform the tests of the grammar-extraction to grammar idea. 
The snakefile will download all necessary repositories
and download the test datasets from pizza and chilli corpus. 
The order of steps might differ due to order in which the rules are performed. 


## Preparation

Install [Snakemake](https://snakemake.github.io), 
you can of course use any package systems for installation.
Then, clone this GitHub project and run snakemake:

```
pip install snakemake
git clone https://github.com/adlerenno/hypercsa-test.git
cd hypercsa-test
snakemake --cores 1
```