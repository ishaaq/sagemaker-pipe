# Summary

This project builds a very simple implementation of SageMaker Training's internal IO
subsystem that is able to pipe channel data files to an algorithm. It is meant to be used
as a local-testing tool in order to test a `PIPE` mode algorithm locally before
attempting to run it for real with SageMaker Training.

Please refer to the [SageMaker docs on writing your own training
algorithms](https://docs.aws.amazon.com/sagemaker/latest/dg/your-algorithms-training-algo.html)
for more details if you don't know what the above means.

## What it does:
Given a single source and destination it will simulate creating a SageMaker Training
Channel and pipe the contents of all the files in the source to the destination via epoch
FIFO files. It loops forever running an infinite number of epochs for the Channel.

## What it does not attempt do:
* It does not attempt to be a performant solution
* It does not emulate `FILE` mode.

# Requirements
You need python3 to run the script. In addition you need to install the requirements
documented in the `requirements.txt` file. Install them via pip like so:
```
[sudo] pip install -r requirements.txt
```


# Usage

```
./sagemaker-pipe.py --help
usage: sagemaker-pipe.py [-h] [-d] [-x] [-r] CHANNEL_NAME SRC DEST

A local testing tool for algorithms that use SageMaker Training in
PIPE mode.

positional arguments:
  CHANNEL_NAME    the name of the channel
  SRC             the source, can be an S3 uri or a local path
  DEST            the destination dir where the data is to be streamed to

optional arguments:
  -h, --help      show this help message and exit
  -d, --debug     enable debug messaging
  -x, --gunzip    inflate gzipped data before streaming it
  -r, --recordio  wrap individual files in recordio records

Examples:

> sagemaker-pipe.py training src-dir dest-dir

The above example will recursively walk through all the files under
src-dir and stream their contents into FIFO files named:
dest-dir/training_0
dest-dir/training_1
dest-dir/training_2
...

> sagemaker-pipe.py train s3://mybucket/prefix dest-dir
This example will recursively walk through all the objects under
s3://mybucket/prefix and similarly stream them into FIFO files:
dest-dir/train_0
dest-dir/train_1
dest-dir/train_2
...
Note that for the above to work the tool needs credentials. You can
set that up either via AWS credentials environment variables:
https://boto3.readthedocs.io/en/latest/guide/configuration.html#environment-variables

OR via a shared credentials file:
https://boto3.readthedocs.io/en/latest/guide/configuration.html#aws-config-file
```

Note that the tool runs with an infinite loop will never exit normally, you wil have to 
stop it manually after your algorithm completes.

If you PIPE-mode algorithm needs to stream from multiple channels simply run multiple
instances of the tool with each pointing to different sources and differing channel names
but the same destination.

Finally run your PIPE-mode algorithm pointing it at `DEST` where it should see a sequence
of FIFO files matching the format `<CHANNEL>_<epoch_num>`.
