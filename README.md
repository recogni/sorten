# sorten

A helper utility to transform data representations for deep learning input.

## Rational

Given a directory of many input files which can be `.jpeg`, `.hdr` and so on, we need an easy way to convert as quickly as possible, all of these images and outputs into more malleable / desirable formats for learning and what-not.  The intention of this is to take advantage of ridiculous CPU and memory offerings from cloud providers while mounting and transforming data stored in google buckets (for example).

## Install

```
$ go get github.com/recogni/sorten
```

Or grab the latest release from the [releases](https://github.com/recogni/sorten/releases) section.

You will also need `imagemagick` installed on your system.  Modern versions of imagemagick will break apart the binary into separate sub-components like `convert`, `compose` and so on.  If you have installed this to a non-standard path, you can set the path to the binaries using the `--magic` option in the command line.

## Usage(s?)

```
$ sorten -h
Usage for sorten:
  -input string
        input directory to read images from
  -magic string
        path to imagemagick binaries (default "/usr/local/bin/")
  -output string
        output directory to read images from
```

Currently `sorten` only supports converting a directory (recursively) of `.hdr` images to `.jpeg`.  Future workers will be crafted to convert intermediate or regular formats between each other.

### `.hdr` -> `.jpeg`

Assuming that an output directory exists at `/mnt/bucket/output`, and a nested directory structure of `.hdr` images live in `/mnt/bucket/input`, you can convert them with as many cores as your system has using the following command:
```
$ sorten -input /mnt/bucket/input -output /mnt/bucket/output
$ sorten -input /mnt/bucket/input -output /mnt/bucket/output -magic /usr/local/bin
```

### UE4 captures -> TF Record

TODO

### JPEG + Bounding boxes -> TF Record

TODO
