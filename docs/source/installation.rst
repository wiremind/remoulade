.. include:: global.rst

Installation
============

Remoulade supports Python versions 3.12 and up and is installable via
`pip`_ or from source.


Via pip
-------
remoulade can be used with a RabbbitMQ_ or a PostgreSQL_ broker.

If you want to use it with RabbitMQ_, simply run the following command in a terminal::

  $ pip install -U 'remoulade[rabbitmq]'

If you want to use PostgreSQL_ with PGMQ_ instead, install::

  $ pip install -U 'remoulade[postgres]'

If you would like to use Redis_-backed extras like results or
cancellation, add the ``redis`` extra to whichever broker you choose::

  $ pip install -U 'remoulade[rabbitmq, redis]'
  $ pip install -U 'remoulade[postgres, redis]'

If you don't have `pip`_ installed, check out `this guide`_.

Extra Requirements
^^^^^^^^^^^^^^^^^^

When installing the package via pip you can specify the following
extra requirements:

=============  =======================================================================================
Name           Description
=============  =======================================================================================
``rabbitmq``   Installs the required dependencies for using Remoulade with RabbitMQ.
``postgres``   Installs the required dependencies for using Remoulade with PostgreSQL and PGMQ.
``redis``      Installs the required dependencies for using Remoulade with Redis.
=============  =======================================================================================

If you want to install Remoulade with all available features, run::

  $ pip install -U 'remoulade[all]'

Optional Requirements
^^^^^^^^^^^^^^^^^^^^^

If you're using Redis as your broker and aren't planning on using PyPy
then you should additionally install the ``hiredis`` package to get an
increase in throughput.


From Source
-----------

To install the latest development version of remoulade from source,
clone the repo from `GitHub`_

::

   $ git clone https://github.com/wiremind/remoulade

then install it to your local site-packages by running

::

   $ python -m pip install .

in the cloned directory.


.. _GitHub: https://github.com/wiremind/remoulade
.. _pip: https://pip.pypa.io/en/stable/
.. _this guide: http://docs.python-guide.org/en/latest/starting/installation/
