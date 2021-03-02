# Redshift benchmarking

A very simply script just showing different ways of working with Redshift, and the benchmark for each. Just used this to compare a few things and show code samples for each of that.

## How to use

```console
# install requirements (preferably in a virtual env!)
pip install -r requirements.txt

# get help
./rs-benchmark.py --help

# execute all scenario's for 1000 records
./rs-benchmark.py insert -h ${DB_HOST} -D ${DB_NAME} -u ${DB_USER} -n 1000
```