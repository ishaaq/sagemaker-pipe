#!/usr/bin/env python3
from pathlib import Path
from urllib.parse import urlparse
import argparse
import errno
import itertools
import os
import shutil
import logging
import boto3


def run(args):
    src = args.src
    dest = args.dest
    channel = args.channel
    print(f'Pipe from src: {src} to dest: {dest} for channel: {channel}')
    if src.startswith("s3://"):
        s3_uri = urlparse(src)
        bucket_str = s3_uri.netloc
        prefix = s3_uri.path.lstrip('/')
        logging.debug(f'bucket: {bucket_str}, prefix: {prefix}')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_str)

        def runner(fifo):
            s3_epoch_runner(bucket, prefix, fifo)
    else:
        def runner(fifo):
            local_epoch_runner(src, fifo)

    run_pipe(dest, channel, runner)


def s3_epoch_runner(bucket, prefix, fifo):
    for obj_summary in bucket.objects.filter(Prefix=prefix):
        logging.debug(f'streaming s3://{bucket.name}/{obj_summary.key}')
        bucket.download_fileobj(obj_summary.key, fifo)


def local_epoch_runner(src_dir, fifo):
    for root, dirs, files in os.walk(src_dir):
        logging.debug(f'file list: {files}')
        for file in files:
            src_path = Path(root, file)
            logging.debug(f'streaming file: {src_path}')
            if src_path.is_file():   # ignore special files
                with src_path.open("br") as src:
                    shutil.copyfileobj(src, fifo)


def run_pipe(input_dir, channel, epoch_runner):
    for epoch in itertools.count():
        print(f'Running epoch: {epoch}')
        # delete previous epoch's fifo if it exists:
        delete_fifo(input_dir, channel, epoch - 1)

        try:
            fifo_path = create_fifo(input_dir, channel, epoch)
            with fifo_path.open(mode='bw', buffering=0) as fifo:
                epoch_runner(fifo)
        finally:
            delete_fifo(input_dir, channel, epoch)
    print(f'Completed pipe for channel: {channel}')


def fifo_path(input_dir, channel, epoch):
    return Path(input_dir, channel + '_' + str(epoch))


def delete_fifo(input_dir, channel, epoch):
    try:
        path = fifo_path(input_dir, channel, epoch)
        path.unlink()
    except OSError as e:
        if e.errno != errno.ENOENT:
            # if the fifo file doesn't exist we don't care, we were going to
            # delete it anyway, otherwise raise:
            raise


def create_fifo(input_dir, channel, epoch):
    path = Path(fifo_path(input_dir, channel, epoch))
    logging.debug(f'Creating fifo: {path}')
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.is_fifo():
        os.mkfifo(path)
    return path


def main():
    parser = argparse.ArgumentParser(
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    description='''
A local testing tool for algorithms that use SageMaker Training in
PIPE mode.
''',
                    epilog='''
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

''')

    parser.add_argument('-d', '--debug', action='store_true',
                        help='enable debug messaging')
    parser.add_argument('-x', '--gunzip', action='store_true',
                        help='inflate gzipped data before streaming it')
    parser.add_argument('-r', '--recordio', action='store_true',
                        help='wrap individual files in recordio records')
    parser.add_argument('channel', metavar='CHANNEL_NAME',
                        help='the name of the channel')
    parser.add_argument('src', metavar='SRC',
                        help='the source, can be an S3 uri or a local path')
    parser.add_argument('dest', metavar='DEST',
                        help='the destination dir where the data is to be \
                        streamed to')
    args, unknown = parser.parse_known_args()

    if unknown:
        logging.warn(f'Ignoring unknown arguments: {unknown}')
    logging.debug(f'Training with configuration: {args}')

    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s',
                            level=logging.DEBUG)

    if args.gunzip:
        logging.warn('gzip inflation not implemented yet - ignoring!')

    if args.recordio:
        logging.warn('recordio wrapping not implemented yet - ignoring!')

    run(args)


if __name__ == '__main__':
    main()
