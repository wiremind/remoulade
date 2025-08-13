# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 WIREMIND SAS <dev@wiremind.fr>
#
# Remoulade is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Remoulade is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import time
from unittest import mock

import remoulade
from remoulade.middleware import TimeLimit, TimeLimitExceeded


@mock.patch("os._exit")
@mock.patch("remoulade.middleware.time_limit.raise_thread_exception")
def test_time_limit_soft(raise_thread_exception, exit, stub_broker, stub_worker, do_work):
    @remoulade.actor(time_limit=1)
    def do_work():
        time.sleep(1)

    stub_broker.declare_actor(do_work)

    # With a TimeLimit middleware
    for middleware in stub_broker.middleware:
        if isinstance(middleware, TimeLimit):
            # That emit a signal every ms and stop after 15ms
            middleware.interval = 1
            middleware.time_limit = 15
            middleware.exit_delay = 0

    # When I send it a message
    do_work.send()

    # And join on the queue
    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    assert raise_thread_exception.called
    assert raise_thread_exception.call_args[0][1] == TimeLimitExceeded
    assert exit.called
