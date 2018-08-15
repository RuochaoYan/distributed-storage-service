## Introduction
This is a cloud-based file storage service called SurfStore. SurfStore is a networked file storage application that supports four basic commands:
- Create a file
- Read the contents of a file
- Change the contents of a file
- Delete a file

Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. Clients accessing SurfStore “see” a consistent set of updates to files, but SurfStore does not offer any guarantees about operations across files, meaning that it does not support multi-file transactions (such as atomic move).

The SurfStore service is composed of the following two sub-services:

- BlockStore: The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. The BlockStore service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.

- MetadataStore: The MetadataStore service holds the mapping of filenames/paths to blocks.

Additionally, there is a client that can support the four basic commands listed above.

There are two versions of this service.
- Centralized version: The metadata service simply keeps its data in memory, with no replication or fault tolerance.

- Distributed version: The MetadataStore service has a set of distributed processes that implement fault tolerance. This distributed implementation uses a replicated log (replicated state machine) plus 2-phase commit to ensure that the MetadataStore service can survive, and continue operating, even if one of its processes fails, and that after failed processes recover they are able to rejoin the distributed system and get up-to-date.


## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
