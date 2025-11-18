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
from pathlib import Path

from freezegun import freeze_time

from remoulade.brokers.stub import StubBroker
from remoulade.middleware import Heartbeat
from remoulade.worker import Worker


def test_heartbeat(stub_broker: StubBroker, do_work, tmp_path: Path):
    beatdir = tmp_path
    stub_broker.add_middleware(Heartbeat(directory=str(beatdir), interval=1))
    stub_broker.emit_after("process_boot")
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    with freeze_time("1998-07-12 21:00"):
        do_work.send()
        stub_broker.join(do_work.queue_name)
        worker.join()

    assert beatdir.is_dir()
    # one thread file
    assert len(list(beatdir.iterdir())) == 1
    beatfile = next(beatdir.iterdir())
    beat = float(beatfile.read_text())

    with freeze_time("1998-07-12 21:00"):
        do_work.send()
        stub_broker.join(do_work.queue_name)
        worker.join()
    assert float(beatfile.read_text()) == beat

    with freeze_time("1998-07-12 21:27"):
        do_work.send()
        stub_broker.join(do_work.queue_name)
        worker.join()
    assert (beat2 := float(beatfile.read_text())) > beat

    # Test it beats even if the queue is empty
    with freeze_time("1998-07-12 23:00"):
        time.sleep(2)
    assert float(beatfile.read_text()) > beat2

    worker.workers[0].stop()
    worker.workers[0].join()
    assert not beatfile.is_file()
    worker.stop()


def test_multith_heartbeat(stub_broker: StubBroker, do_work, tmp_path: Path):
    beatdir = tmp_path
    stub_broker.add_middleware(Heartbeat(directory=str(beatdir), interval=1))
    stub_broker.emit_after("process_boot")
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=2)
    worker.start()
    t0 = worker.workers[0]
    t1 = worker.workers[1]
    t0.pause()
    t0.paused_event.wait()
    do_work.send()
    stub_broker.join(do_work.queue_name)
    worker.join()

    t1.pause()
    t1.paused_event.wait()
    t0.resume()
    do_work.send()
    stub_broker.join(do_work.queue_name)
    worker.join()

    assert beatdir.is_dir()
    # two thread files
    assert len(list(beatdir.iterdir())) == 2
    assert all(float(f.read_text()) for f in beatdir.iterdir())

    t0.stop()
    t0.join()
    assert not any(str(t0.ident) in str(p) for p in beatdir.iterdir())
    assert len(list(beatdir.iterdir())) == 1
    worker.stop()
