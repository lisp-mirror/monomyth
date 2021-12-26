# Monomyth

Monomyth is a distributed data processing system built using Common Lisp and
based heavily on [Broadway](https://hexdocs.pm/broadway/Broadway.html).
It is designed to split the messaging systems into two, one defined and
controlled by Monomyth, and one defined and controlled by the user.
The messaging controlled by the user pertains only to data, which moves between
persisted data streams and node threads.
The structure and manipulation of this data is largely defined by the user
(though there are data stream specific aspects certain node types might handle).
Monomyth itself handles all aspects of system orchestration via the Monomyth
Orchestration Protocol (MMOP).
The work itself is done on a group of distributed workers that use concurrent,
user defined nodes to process the data and are controlled by a single master server.

Design documentation can be found at `design/doc.org`.

## Tests

To run the tests you will need to have `roswell` and `qlot` installed, the tests
have been verified on SBCL 2.1.11.
Then perform the following:
```bash
source test_env.sh
docker-compose up -d
qlot install
qlot exec ./bin/test.ros
```

## Example

### Basic

The basic example right now is pretty arbitrary, both in set up and the
computations involved.
In one process start `qlot exec ./bin/example-master.ros`, and then in a few
others run `qlot exec ./bin/example-worker.ros`.
Once all the workers have connected to the master (you will see log entries),
press the return button in the first process.

### Map/Reduce

The map reduce example is also filled with shortfalls, but at least is closer to
a real world example.
The entire thing can be run through an included script, just run
`qlot exec ./bin/run-map-reduce.ros`.

## Status

This project has its basic architecture set up, but lacks most of the functionality
needed for a 1.0 release.

The features likely to be targeted for a 1.0 release:
- Support for sending the results of a node to multiple queues.
- DSL support for sending multiple nodes to a single queue.
- DSL support filtering which message goes to which queue.
- DSL support for iteration.
- Support for nodes not picking messages off queue (for generators).
- Basic heartbeat and restart for workers.
- Basic monitoring of workers and nodes, possibly data flow.
- More fine grained control of node threads.
- Possible (but very unlikely) Kafka support.

THIS IS A TEST
