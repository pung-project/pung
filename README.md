# Pung: Unobservable communication over fully untrusted infrastructure
[![Build Status](https://travis-ci.org/pung-project/pung.svg?branch=master)](https://travis-ci.org/pung-project/pung)

Pung is a (research) communication system where users can privately talk to each other
over a fully untrusted channel. 
In particular, Pung provably hides all content and metadata in
a conversation while withstanding global adversaries. 
See our [paper](https://www.usenix.org/system/files/conference/osdi16/osdi16-angel.pdf)
for details about the protocol and its guarantees.


# Compiling Pung

Pung is written in [Rust](https://www.rust-lang.org/) and compiles under the nightly compiler (nighly
  is required for our benchmarking framework).
If you wish to use the nightly compiler as default simply run:

```sh
$ rustup install nightly
$ rustup default nightly
```

Alternatively, you can run the following from the Pung directory to set the nightly
compiler only for Pung.

```sh
$ rustup install nightly
$ rustup override set nightly
```


## Dependencies

Pung depends on [Cap'n Proto](https://capnproto.org) for message serialization,
and a modified version of [XPIR](https://github.com/XPIR-team/XPIR). To get both
dependencies simply run the following within Pung's directory:

```sh
$ git submodule init
$ git submodule update
```


## Installing Cap'n Proto

```sh
$ cd deps/capnproto/c++
$ ./setup-autotools.sh
$ autoreconf -i
$ ./configure
$ make check
$ sudo make install
```

## Installing XPIR's dependencies

XPIR depends on boost >= 1.55, gmp, and mpfr. You can install them as follows.

Ubuntu 14.04 (or later) / Debian:

```sh
$ sudo apt-get install libboost1.55-all-dev libmpfr-dev libgmp-dev
```

Arch:
```sh
$ sudo pacman -S boost mpfr gmp
```

Gentoo:
```sh
$ sudo emerge dev-libs/boost dev-libs/mpfr dev-libs/gmp
```

**Note:** there is no need to compile or install XPIR. It will be built and linked 
automatically when Pung is compiled (see below).

## Compiling Pung's binaries 

To compile Pung with debug symbols simply run: ``$ cargo build``. The resulting 
binaries will be: ``target/debug/client`` and ``target/debug/server``.
To compile PUng with compiler optimizations run: ``$ cargo build --release``. The resulting
binaries will be: ``target/release/client`` and ``target/release/server``.


# Running Pung's microbenchmarks

All our microbenchmarks are found in the ``benches`` folder. 
We use the [Criterion] (https://github.com/japaric/criterion.rs) library to measure
all running times and provide useful statistics.

To run all microbenchmarks, simply type: ``$ cargo bench -- --test --nocapture bench``. 
The results will be found in the ``.criterion`` folder.

To run a single (or a set of) microbenchmark, simply type: ``$ cargo bench -- --test --nocapture [PREFIX]``.
Where [PREFIX] is a prefix of the name of the microbenchmark(s) that you wish to run. For instance,
since all our microbenchmarks start with "bench", using "bench" as the prefix runs all of them. However,
if one wished to only run the PIR microbenchmarks it is sufficient to pass in "bench\_pir" as the prefix.

# Running Pung

The first step is to launch the server (the -m flag tells it to expect 2 messages each round): 

```sh
$ ./target/release/server -m 2
```

Pass in --help to see available options. The second step is to launch the client: 

```sh
$ ./target/release/client -n "user1" -p "user2" -x "secret" -r 10 &
$ ./target/release/client -n "user2" -p "user1" -x "secret" -r 10
```

The above will run two clients with ids "user1" and "user2" that will communicate with
each other using secret "secret" for 10 rounds. One can also run a single client by having it
talk to itself (i.e., passing the same argument to -n and -p for the client and 
setting the server to expect only one message: -m 1).


Pass in --help to see available options. It is important that the client and the server
are run with the same options (e.g., retrieval type, optimization, number of buckets).

See ``launch_clients.py`` and ``launch_servers.py`` in ``scripts`` for instructions on how
to run multiple clients and servers.
