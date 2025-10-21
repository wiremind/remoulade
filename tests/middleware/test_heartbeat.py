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

from remoulade.brokers.stub import StubBroker
from remoulade.middleware import Heartbeat
from remoulade.worker import Worker


def test_heartbeat(stub_broker: StubBroker, do_work, tmp_path: Path):
    beatdir = tmp_path
    stub_broker.add_middleware(Heartbeat(directory=str(beatdir), interval=1))
    stub_broker.emit_after("process_boot")
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=1)
    worker.start()

    do_work.send()

    stub_broker.join(do_work.queue_name)
    worker.join()

    assert beatdir.is_dir()
    # one proc dir
    assert len(list(beatdir.iterdir())) == 1
    # one thread file
    procdir = next(beatdir.iterdir())
    assert len(list(procdir.iterdir())) == 1
    beatfile = next(procdir.iterdir())
    beat = float(beatfile.read_text())

    do_work.send()
    stub_broker.join(do_work.queue_name)
    worker.join()
    # This is flaky if it took more than 1 second to get here since the previous send
    # But I guess there's bigger issues if it is the case.
    assert float(beatfile.read_text()) == beat

    time.sleep(1)
    do_work.send()
    stub_broker.join(do_work.queue_name)
    worker.join()
    assert (beat2 := float(beatfile.read_text())) > beat

    # Test it beats even if the queue is empty
    # Flaky due to lack of sync, but should not actually fail
    time.sleep(2)
    assert float(beatfile.read_text()) > beat2

    worker.workers[0].stop()
    worker.workers[0].join()
    assert not beatfile.is_file()
    worker.stop()
    assert not procdir.is_dir()


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
    # one proc dir
    assert len(list(beatdir.iterdir())) == 1
    # two thread file
    procdir = next(beatdir.iterdir())
    assert len(list(procdir.iterdir())) == 2
    assert all(float(f.read_text()) for f in procdir.iterdir())

    t0.stop()
    t0.join()
    assert not any(str(t0.ident) in str(p) for p in procdir.iterdir())
    assert len(list(procdir.iterdir())) == 1
    worker.stop()
    assert not procdir.is_dir()
