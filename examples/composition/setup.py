# Dummy python package to show composition

from setuptools import setup


setup(
    name="composition",
    version="0.0.1",
    packages=[
        "composition",
    ],
    package_dir={"composition": "."},
    install_requires=[
        "remoulade[redis,rabbitmq,postgres,server]",
        "prometheus_client",
        "requests",
    ],
    entry_points={"console_scripts": [
        "composition_worker=composition.worker:run",
        "composition_ask_count_words=composition.count_words:ask_count_words",
        "composition_serve=composition.serve:serve",
    ]},
)
