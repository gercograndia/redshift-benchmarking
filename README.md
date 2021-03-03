# Redshift benchmarking

A very simply script just showing different ways of working with Redshift, and the benchmark for each. Just used this to compare a few things and show code samples for each of that.

## How to use

```console
# install requirements (preferably in a virtual env!)
pip install -r requirements.txt

# get help
./rs-benchmark.py --help

# execute all scenario's for 1000 records (note, this has to be run from an AWS context)
./rs-benchmark.py insert -h ${DB_HOST} -D ${DB_NAME} -u ${DB_USER} -n 1000 \
    -c s3://${S3_PATH} -i ${IAM_ROLE}
```
