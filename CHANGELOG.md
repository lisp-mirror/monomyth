# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Nodes no longer wait at all between iterations, but the rmq-node timeout on
  pulling data from rabbit-mq has been raised to 0.1 seconds.
  This should result in about the same behavior in terms of shutdown (though
  slower) but vastly decrease processing time of longer processes.
  Test time, however, has increased since node now wait longer to exit.
- Nodes now wait for the iteration to be 'finished' when they complete a task.
- The node methods `startup` and `shutdown` have been renamed to `start-node`
  `stop-node` for greater clarity and consistency.
  This is an internal change and should have no effect on the user API.

### Fixed
- The worker thread and master routing threads now block properly instead of
  constantly polling and re-polling the socket.
- The content-type header in the control api has been fixed.

## [0.4.1] - 2021-12-25
### Fixed
- Removed broken dependency (lucerne) with clack and ningle in control api.

## [0.4.0] - 2021-12-24
### Added
- Nodes now support not placing items in a destination queue.
- Nodes now support not pulling items in a source queue.
- Nodes now support completing a bounded stream and the recipe-info endpoint will
  report completed nodes.
- The DSL now supports extending the node startup and shutdown functions.
  These functions should take no arguments and run at the start of the startup
  sequence and the end of the shutdown sequence respectively.
- Node recipes now contain a list of 'dependent' nodes (nodes next in the system).
  The DSL will construct these automatically.
- Map/reduce example.

### Changed
- Node shutdown now attempts to kill the worker thread and wait to complete their
  last cycle before shutting down further.
- The dsl now takes a system tame.
- The dsl now builds an 'add-<system-name>-recipes' function that is needed to add
  the recipes to the master server.

### Removed
- RMQ node recipes no longer control destination and source queues, these are
  now set in the dsl via the `build-node` functions.
- Node recipes no longer control batch size, these are no set in the dsl via the
  `build-node` functions.

### Fixed
- The recipe info endpoint on the control api now lists all recipes regardless of
  whether they have ever been used or not.

## [0.3.1] - 2020-11-11
### Fixed
- Fixed bug where the trivia match call in `mmop-control` couldn't build.

## [0.3.0] - 2020-11-02
### Added
- Built define rmq node macro that constructs all of the classes, constructors,
  and also builds the methods that support building and running nodes.
- Built define system macro that takes a list of node info an constructs all classes
  and queues.
- Recipes built by `define-system` are now automatically stored in the master server
  at start up.

## [0.2.0] - 2020-10-13
### Added
- Control REST API that allows users to manage the system.
- Control REST API ping endpoint.
- Control REST API start-node endpoint.
- Control REST API stop-worker endpoint.
- Control REST API recipe-info endpoint.

### Changed
- MMOP structs changed to ADTs.
- Master worker threads now using dealer sockets (allowing for bi-directional communication
  through the router), but the load balancing is round-robin.
- Changed to using qlot to manage dependencies.

### Removed
- Removed communication tests (processing tests should include verifying that communication works).

## [0.1.0] - 2020-09-13
### Added
- Basic extendable node architecture that builds on a four step system, pull items,
  transform items, place items, and handling failures based on previous steps.
- Basic rabbit-mq node with a bear-bones implementation.
- Started basic high level docs.
- Added MMOP/0 worker-ready.
- Added MMOP/0 start-node.
- Added MMOP/0 start-node-success.
- Added MMOP/0 start-node-failure.
- Added MMOP/0 stop-worker.
- Added node recipe system for constructing node threads.
- rmq-worker startup/shutdown, node start up, and event loop.
- Added test suites designed to test communication over a network.
- rmq-nodes now support graceful shutdown and are wrapped by stmx for multi-threaded operations.
- Added test suites designed to test multi-node processing over a network.
