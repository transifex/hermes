Python hermes-py Documentation
==============================
Hermes is a Postgres-talking, event-driven, failure-handling Python library.
Its main purpose is to enable the easy implementation of resilient Python
processes which require communication with Postgres. It defines a base-layer
which you can build as little or as much as you like on top of.

It's been used at Transifex to fulfil a number of roles, one of them
including a Postgres -> Elasticsearch river.

Compatibility
-------------
*nix operating system which supports the select function.

Postgresql 9.0+ is required to support `LISTEN/NOTIFY <http://www.postgresql.org/docs/current/static/sql-notify.html>`_ commands.

Installation
------------
::

    pip install hermes-pg

Usage
-----
Most users will just need to define some form of process to run when an event
is emitted. This can be achieved by defining a processor object and supplying
that to the Client object like so::

    from hermes.components import Component

    class Processor(Component):
        def __init__(self,)
            super(Processor, self).__init__()

        def execute(self):
            # Do some amazing event-driven stuff
            ...




Contents
--------
.. toctree::
   :maxdepth: 2

   client
   components
   connectors
   listeners
   strategies
   exceptions
   Changelog


Indices and tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
