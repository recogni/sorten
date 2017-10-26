# sorten

A helper utility to transform data representations for deep learning input.

## Rationale

Given a directory of many input files which can be `.jpeg`, `.hdr` and so on, we need an easy way to convert as quickly as possible, all of these images and outputs into more malleable / desirable formats for learning and what-not.  The intention of this is to take advantage of ridiculous CPU and memory offerings from cloud providers while mounting and transforming data stored in google buckets (for example).

## Install

```
$ go get github.com/recogni/sorten
```

Or grab the latest release from the [releases](https://github.com/recogni/sorten/releases) section.

The following additional CLI tools will also be required to run some of the image conversion jobs:

`imagemagick`:

You will also need `imagemagick` installed on your system.  Modern versions of imagemagick will break apart the binary into separate sub-components like `convert`, `compose` and so on.  If you have installed this to a non-standard path, you can set the path to the binaries using the `--magic` option in the command line.
```
brew install imagemagick
```

`luminence-hdr`

This is needed to apply the correct and appropriate tonemapping operator on the input HDR images, which will then give us losless 8-bit tiff images. From here we use `imagemagick` to convert again into jpeg.
Here is an interesting discussion on tonemapping: http://osp.wikidot.com/parameters-for-photographers.

Note: On OSX 10.9 you will need an older (2.2.1) version of this program.

http://qtpfsgui.sourceforge.net/?page_id=10

## Cross compile

```
$ cd $GOPATH/src/github.com/recogni/sorten
$ GOOS=linux go build -o sorten_linux .
```

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
