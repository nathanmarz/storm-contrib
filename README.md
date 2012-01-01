Introduction
============

Storm-contrib is a set of modules for performing common tasks in Storm,
and particularly for integrating with various 3rd-party tools. These are
wheels that should not need to be re-invented by every developer who
wants to build something non-trivial on top of Storm.

For more information about Storm itself, see [the Storm GitHub
repository](http://github.com/nathanmarz/storm).

Modules
=======

Storm-contrib currently consists of the following modules:

storm-contrib-core
------------------

General-purpose tools (for example, a ClockSpout) that implement
patterns used by various other modules.

storm-contrib-mongo
-------------------

A simple pattern for writing records from a Storm stream to a MongoDB
collection.
