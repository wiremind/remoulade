.. include:: global.rst

Remoulade: simple task processing
=================================

Release v\ |release|. (:doc:`installation`, :doc:`changelog`, `Source Code`_)

.. _Source Code: https://github.com/wiremind/remoulade

.. image:: https://img.shields.io/badge/license-LGPL-blue.svg
   :target: license.html
.. image:: https://circleci.com/gh/wiremind/remoulade.svg?style=svg
   :target: https://circleci.com/gh/wiremind/remoulade
.. image:: https://badge.fury.io/py/remoulade.svg
   :target: https://badge.fury.io/py/remoulade

**Remoulade** is a distributed task processing library for Python with
a focus on simplicity, reliability and performance.
This is a fork of Dramatiq_.

Here's what it looks like:

::

  import remoulade
  import requests

  @remoulade.actor
  def count_words(url):
     response = requests.get(url)
     count = len(response.text.split(" "))
     print(f"There are {count} words at {url!r}.")

  # Synchronously count the words on example.com in the current process
  count_words("http://example.com")

  # or send the actor a message so that it may perform the count
  # later, in a separate process.
  count_words.send("http://example.com")

**Remoulade** is :doc:`licensed<license>` under the LGPL and it
officially supports Python 3.5 and later.


Get It Now
----------

If you want to use it with RabbitMQ_::

   $ pip install -U 'remoulade[rabbitmq, watch]'

Or if you want to use it with Redis_::

   $ pip install -U 'remoulade[redis, watch]'

Read the :doc:`motivation` behind it or the :doc:`guide` if you're
ready to get started.


User Guide
----------

This part of the documentation is focused primarily on teaching you
how to use Remoulade.

.. toctree::
   :maxdepth: 2

   installation
   motivation
   guide
   best_practices
   advanced
   cookbook


API Reference
-------------

This part of the documentation is focused on detailing the various
bits and pieces of the Remoulade developer interface.

.. toctree::
   :maxdepth: 2

   reference


Project Info
------------

.. toctree::
   :maxdepth: 1

   Source Code <https://github.com/wiremind/remoulade>
   changelog
   Contributing <https://github.com/wiremind/remoulade/blob/master/CONTRIBUTING.md>
   license
