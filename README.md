# Pantry

[![Crates.io](https://img.shields.io/crates/v/pantry.svg)](https://crates.io/crates/pantry)
[![Documentation](https://docs.rs/pantry/badge.svg)][dox]

The `Pantry` is useful for temporarily storing for later use values that
might "decay" (become unusable) over time, such as:

* network connections kept alive for later reuse if the peer doesn't close
  their end
* files which may be deleted from the filesystem by the user or other programs
* "weak references" to resources which may be released at any point by other
  actors

More information can be found in the [crate documentation][dox].

[dox]: https://docs.rs/pantry

## Usage

Create a `Pantry` value and use its `store` function to store values for later
use.  A key is provided along with the value, so that the value can be
retrieved later using the same key.  A worker thread is spawned which monitors
the values and automatically drops any which have "decayed".  Values must
implement the `Perishable` trait, whose `perished` function asynchronously
completes once the value has decayed.

Use the `fetch` asynchronous function on the `Pantry` with a key to retrieve a
value previously stored using that key.  A value is only returned if it was
stored using the same key and has not decayed since it was stored.

Multiple values may have the same key, with the caveat that the values stored
under a given key may not be returned in the same order in which they were
stored.
