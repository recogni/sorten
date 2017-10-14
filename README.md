# sorten

A helper utility to transform data representations for deep learning input.

## Rational

Given a directory of many input files which can be `.jpeg`, `.hdr` and so on, we need an easy way to convert as quickly as possible, all of these images and outputs into more malleable / desirable formats for learning and what-not.

## Install

```
go get github.com/recogni/sorten
```

Or grab the latest release from the [releases section]()

## Usages

Currently `sorten` only supports converting a directory (recursively) of `.hdr` images to `.jpeg`.  Future workers will be crafted to convert intermediate or regular formats between each other.

### `.hdr` -> `.jpeg`

Assuming that an output directory exists at `/mnt/bucket/output`, and a nested directory structure of `.hdr` images live in `/mnt/bucket/input`, you can convert them with as many cores as your system has using the following command:
```

```

### UE4 captures -> TF Record

### JPEG + Bounding boxes -> TF Record
