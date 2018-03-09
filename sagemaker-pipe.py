#!/usr/bin/env python3

from pathlib import Path
from urllib.parse import urlparse
import argparse
import boto3
import errno
import gzip
import itertools
import logging
import os
import shutil


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

        def src_retriever(sink):
            s3_retriever(bucket, prefix, sink)
    else:
        def src_retriever(sink):
            local_retriever(src, sink)

    if args.gunzip:
        def unzipper(sink):
            tmp_path = Path(dest, '.' + channel + '.tmp')
            gunzip(src_retriever, tmp_path, sink)
            tmp_path.unlink()
        run_pipe(channel, unzipper, dest)
    else:
        run_pipe(channel, src_retriever, dest)


def s3_retriever(bucket, prefix, sink):
    for obj_summary in bucket.objects.filter(Prefix=prefix):
        logging.debug(f'streaming s3://{bucket.name}/{obj_summary.key}')
        bucket.download_fileobj(obj_summary.key, sink)


def local_retriever(src, sink):
    if os.path.isfile(src):
        logging.debug(f'streaming file: {src}')
        with open(src, 'rb') as src:
            shutil.copyfileobj(src, sink)
    else:
        for root, dirs, files in os.walk(src):
            logging.debug(f'file list: {files}')
            for file in files:
                src_path = Path(root, file)
                logging.debug(f'streaming file: {src_path}')
                if src_path.is_file():   # ignore special files
                    with src_path.open('rb') as src:
                        shutil.copyfileobj(src, sink)


def gunzip(src_retriever, tmp_path, sink):
    with open(tmp_path, 'wb') as tmp:
        src_retriever(tmp)
    with gzip.open(tmp_path, 'rb') as inflated:
        shutil.copyfileobj(inflated, sink)


def run_pipe(channel, src_retriever, dest):
    for epoch in itertools.count():
        print(f'Running epoch: {epoch}')
        # delete previous epoch's fifo if it exists:
        delete_fifo(dest, channel, epoch - 1)

        try:
            fifo_pth = create_fifo(dest, channel, epoch)
            with fifo_pth.open(mode='bw', buffering=0) as fifo:
                src_retriever(fifo)
        finally:
            delete_fifo(dest, channel, epoch)
    print(f'Completed pipe for channel: {channel}')


def fifo_path(dest, channel, epoch):
    return Path(dest, channel + '_' + str(epoch))


def delete_fifo(dest, channel, epoch):
    try:
        path = fifo_path(dest, channel, epoch)
        path.unlink()
    except OSError as e:
        if e.errno != errno.ENOENT:
            # if the fifo file doesn't exist we don't care, we were going to
            # delete it anyway, otherwise raise:
            raise


def create_fifo(dest, channel, epoch):
    path = Path(fifo_path(dest, channel, epoch))
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
        logging.warning(f'Ignoring unknown arguments: {unknown}')
    logging.debug(f'Training with configuration: {args}')

    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s',
                            level=logging.DEBUG)

    if args.recordio:
        logging.warning('recordio wrapping not implemented yet - ignoring!')

    run(args)


if __name__ == '__main__':
    main()
