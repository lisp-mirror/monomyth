# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
- Added test suites designed to test multi machine processing.
