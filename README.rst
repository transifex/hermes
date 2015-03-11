Hermes
=======
Hermes is a Postgres-talking, event-driven, failure-handling Python library.
Its main purpose is to enable the easy implementation of resilient Python
processes which require communication with Postgres. It defines a base-layer
which you can build as little or as much as you like on top of.

It's been used at Transifex to fulfil a number of roles, one of them
including a Postgres -> Elasticsearch river.


Contribute
----------
If you'd like to contribute, then fire up your IDE, do something awesome and submit a PR.

You can run tests by doing the following::

	python setup.py test

Status
------
.. image:: https://circleci.com/gh/transifex/hermes.svg?style=badge
    :target: https://circleci.com/gh/transifex/hermes

.. image:: https://coveralls.io/repos/transifex/hermes/badge.svg?branch=be_graceful
    :target: https://coveralls.io/r/transifex/hermes?branch=be_graceful
