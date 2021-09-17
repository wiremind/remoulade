from remoulade.state.backends.postgres import DB_VERSION, StateVersion, StoredState
from tests.conftest import check_postgres


def test_no_changes(stub_broker, postgres_state_middleware, check_postgres_begin):
    backend = postgres_state_middleware.backend
    client = backend.client
    with client.begin() as session:
        session.add(StoredState(message_id="id"))

    backend.init_db()
    assert check_postgres(client)
    with client.begin() as session:
        assert len(session.query(StoredState).all()) == 1


def test_create_tables(stub_broker, postgres_state_middleware, check_postgres_begin):
    backend = postgres_state_middleware.backend
    client = backend.client
    with client.begin() as session:
        engine = session.get_bind()
        StoredState.__table__.drop(bind=engine)
        StateVersion.__table__.drop(bind=engine)

    backend.init_db()
    assert check_postgres(client)


def test_change_version(stub_broker, postgres_state_middleware, check_postgres_begin):
    backend = postgres_state_middleware.backend
    client = backend.client
    with client.begin() as session:
        version = session.query(StateVersion).first()
        version.version = DB_VERSION + 1
        session.add(StoredState(message_id="id"))

    backend.init_db()
    assert check_postgres(client)
    with client.begin() as session:
        assert len(session.query(StoredState).all()) == 0


def test_no_version(stub_broker, postgres_state_middleware, check_postgres_begin):
    backend = postgres_state_middleware.backend
    client = backend.client
    with client.begin() as session:
        engine = session.get_bind()
        StateVersion.__table__.drop(bind=engine)
        session.add(StoredState(message_id="id"))

    backend.init_db()
    assert check_postgres(client)
    with client.begin() as session:
        assert len(session.query(StoredState).all()) == 0


def test_no_states(stub_broker, postgres_state_middleware, check_postgres_begin):
    backend = postgres_state_middleware.backend
    client = backend.client
    with client.begin() as session:
        engine = session.get_bind()
        StoredState.__table__.drop(bind=engine)

    backend.init_db()
    assert check_postgres(client)
