# Summary

This project builds a very simple implementation of SageMaker Training's internal IO
subsystem that is able to pipe multi-channel data files to an algorithm container. It is
meant to be used as a local-testing tool in order to test a `PIPE` mode algorithm locally
before attempting to run it for real in a SageMaker Training environment.

## What it does:
Given a directory with multiple subdirectories (upto 8) it will simulate creating a
SageMaker Training Channel matching the name of each directory and pipe the contents of
all the files within that directory to a Channel-specific fifo. It loops forever running
an infinite number of epochs for each Channel.

## What it does not attempt do:
* It does not attempt to be a performant solution
* It does not stream files from S3 to the algorithm container
* It does not emulate `FILE` mode.

# Building
Build it with `docker build`:

```
docker build -t dataagent
```

# Running a local training cluster

Prior to starting up the local training cluster you need to setup a few
directories and files. Here is an example directory setup:

```
$ROOT_DIR
├── input
│   ├── config
│   │  └── hyperparameters.json     # fill in any hyperparameters needed by
│   │                               # your algo
│   └── data
├── model
│   └── data                        # your algo's model should show up here
├── model
└── src-data                        # Each subdir under this dir corresponds to a single
    │                               # training Channel, as per SageMaker docs, there is
    │                               # an 8-channel limit. The names of the subdirs will
    │                               # be the names of the Channels, the names below are
    │                               # merely representative, replace them as you wish.
    ├── training
    │   │
    │   ├── training-data-file1     # put as many data files you would like to test
    │   ├── training-data-file2
    │   └── training-data-file3
    ├── testing
    │   └── training-data-file1
    └── validation
        └── training-data-file1
```

You can then run the dataagent as a container mounting `/src-data` and `/opt/ml/input`:
```
docker run -d --rm -v $ROOT_DIR/src-data:/src-data \
                -v $ROOT_DIR/input:/opt/ml/input \
                --name dataagent dataaget
```

Note that the dataagent runs with an infinite loop on each Channel and will never exit
normally, you will have to stop the container manually after your algorithm completes.

Then, assuming you have a PIPE-mode algorithm docker image named `$ALGO` you can run the
algo container. You need to mount `/opt/ml/input` and `/opt/ml/model`:

```
docker run --rm -v $ROOT_DIR/input:/opt/ml/input \
                -v $ROOT_DIR/model:/opt/ml/model \
                --name algo $ALGO
```

If your algorithm completes training successfully and writes a model conforming to
SageMaker spec you should see the model under `$ROOT_DIR/model`.

Please refer to the [SageMaker docs on writing your own training
algorithms](https://docs.aws.amazon.com/sagemaker/latest/dg/your-algorithms-training-algo.html)
for more details.
