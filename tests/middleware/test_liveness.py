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
from time import sleep
from urllib import request

import pytest

from remoulade.brokers.stub import StubBroker
from remoulade.middleware import Liveness


def test_liveness_middleware_exposes_route():
    # Given a broker
    broker = StubBroker()

    # And an instance of the prometheus middleware
    liveness = Liveness()
    liveness.after_process_boot(broker)

    # If I wait for the server to start
    sleep(0.01)

    # When I request the health via HTTP
    with request.urlopen("http://127.0.0.1:8080") as resp:
        # Then the response should be successful
        assert resp.getcode() == 200

    # If i stop the service
    liveness.after_worker_shutdown(broker, None)

    with pytest.raises(Exception):
        # When I request the health via HTTP
        # I get no response
        request.urlopen("http://127.0.0.1:8080", timeout=0.05)

