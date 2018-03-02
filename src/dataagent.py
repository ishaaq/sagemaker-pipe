#!/usr/bin/env python3
"""
TODO: documentation
"""
import argparse
import os
import errno
import itertools
import shutil
from pathlib import Path
from multiprocessing import Pool
import traceback


def run_pipes(src_dir, input_dir):
    channels = os.listdir(src_dir)
    print(f'Found channels: {channels}')
    validate(src_dir, channels)

    with Pool(len(channels)) as pool:
        pipe_args = [(src_dir, input_dir, channel) for channel in channels]
        pool.starmap(run_pipe, pipe_args)
        # TODO error stack from sub processes


def validate(src_dir, channels):
    print("validating")
    if len(channels) > 8:
        raise Exception('Invalid number of channels, max 8 channels allowed')

    for channel in channels:
        channel_path = Path(src_dir, channel)
        print(f'checking {channel_path}')
        if not channel_path.is_dir():
            raise Exception(f'The channel {channel_path} is not a directory')


def run_pipe(src_dir, input_dir, channel):
    try:
        print(f'Running pipe for channel: {channel}')
        channel_data = os.path.join(src_dir, channel)
        for epoch in itertools.count():
            print(f'Running \'{channel}\' epoch: {epoch}')
            # delete previous epoch's fifo if it exists:
            delete_fifo(input_dir, channel, epoch - 1)

            try:
                fifo_path = create_fifo(input_dir, channel, epoch)
                with fifo_path.open(mode='bw', buffering=0) as fifo:
                    for root, dirs, files in os.walk(channel_data):
                        print(f'file list: {files}')
                        for file in files:
                            src_path = Path(root, file)
                            print(f'streaming file: {src_path}')
                            if src_path.is_file():   # ignore special files
                                with src_path.open("br") as src:
                                    shutil.copyfileobj(src, fifo)
            finally:
                delete_fifo(input_dir, channel, epoch)
        print(f'Completed pipe for channel: {channel}')
    except:
        traceback.print_exc()


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
    if not path.is_fifo():
        os.mkfifo(path)
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_dir', metavar='SRC_DIR',
                        help='the dir containing the source data to send to \
                        the algorithm container (default: %(default)s)',
                        nargs='?', default='/src-data')
    parser.add_argument('--input_dir', metavar='INPUT_DIR',
                        help='the input dir where the algorithm container is \
                        expecting data (default: %(default)s)',
                        nargs='?', default='/opt/ml/input/data')
    args, unknown = parser.parse_known_args()
    print(f'Ignoring unknown arguments: {unknown}')
    print(f'Training with configuration: src_dir: {args.src_dir}, input_dir: {args.input_dir}')

    run_pipes(args.src_dir, args.input_dir)


if __name__ == '__main__':
    main()
