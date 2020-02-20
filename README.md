
Kasparov
====
Warning: This is pre-alpha software. There's no guarantee anything works.
====

[![ISC License](http://img.shields.io/badge/license-ISC-blue.svg)](https://choosealicense.com/licenses/isc/)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/kaspanet/kasparov)

Kasparov is an API server for Kaspa written in Go (golang).

This project contains the following executables:
- kasparovd - the Kasparov server. Handles user requests.
- kasparovsyncd - the Kasparov sync daemon. Maintains sync with a full Kaspa node.
- examples/wallet - an example Kaspa wallet. Interfaces with a kasparovd instance. 

This project is currently under active development and is in a pre-Alpha state. 
Some things still don't work and APIs are far from finalized. The code is provided for reference only.

## Requirements

Latest version of [Go](http://golang.org) (currently 1.13).

## Installation

#### Build from Source

- Install Go according to the installation instructions here:
  http://golang.org/doc/install

- Ensure Go was installed properly and is a supported version:

```bash
$ go version
$ go env GOROOT GOPATH
```

NOTE: The `GOROOT` and `GOPATH` above must not be the same path. It is
recommended that `GOPATH` is set to a directory in your home directory such as
`~/dev/go` to avoid write permission issues. It is also recommended to add
`$GOPATH/bin` to your `PATH` at this point.

- Run the following commands to obtain and install kasparovd, kasparovsyncd, and the wallet including all dependencies:

```bash
$ git clone https://github.com/kaspanet/kasparov $GOPATH/src/github.com/kaspanet/kasparov
$ cd $GOPATH/src/github.com/kaspanet/kasparov
$ go install ./...
```

- kasparovd, kasparovsyncd, and the wallet should now be installed in `$GOPATH/bin`. If you did
  not already add the bin directory to your system path during Go installation,
  you are encouraged to do so now.


## Getting Started

Kasparov expects to have access to the following systems:
- A Kaspa RPC server (usually [kaspad](https://github.com/kaspanet/kaspad) with RPC turned on)
- A MySQL database
- An optional MQTT broker

### Linux/BSD/POSIX/Source

#### kasparovd

```bash
$ ./kasparovd --rpcserver=localhost:16210 --rpccert=path/to/rpc.cert --rpcuser=user --rpcpass=pass --dbuser=user --dbpass=pass --dbaddress=localhost:3306 --dbname=kasparov --testnet
```

#### kasparovsyncd

```bash
$ ./kasparovsyncd --rpcserver=localhost:16210 --rpccert=path/to/rpc.cert --rpcuser=user --rpcpass=pass --dbuser=user --dbpass=pass --dbaddress=localhost:3306 --dbname=kasparov --migrate --testnet
$ ./kasparovsyncd --rpcserver=localhost:16210 --rpccert=path/to/rpc.cert --rpcuser=user --rpcpass=pass --dbuser=user --dbpass=pass --dbaddress=localhost:3306 --dbname=kasparov --mqttaddress=localhost:1883 --mqttuser=user --mqttpass=pass --testnet
```

#### wallet

See the full [wallet documentation](https://docs.kas.pa/kaspa/try-kaspa/cli-wallet).

## Discord
Join our discord server using the following link: https://discord.gg/WmGhhzk

## Issue Tracker

The [integrated github issue tracker](https://github.com/kaspanet/kasparov/issues)
is used for this project.

## License

Kasparov is licensed under the copyfree [ISC License](https://choosealicense.com/licenses/isc/).

