import os

from setuptools import setup


def rel(*xs):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *xs)


with open(rel("README.md")) as f:
    long_description = f.read()


with open(rel("remoulade", "__init__.py")) as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")


dependencies = ["prometheus-client>=0.2", "pytz", "python-dateutil>=2.8.0", "typing-extensions>=3.7"]

extra_dependencies = {
    "rabbitmq": ["amqpstorm>=2.6,<3"],
    "redis": ["redis>=3.5.0,<4.0"],
    "server": ["flask>=1.1,<2.2", "marshmallow>=3", "flask-apispec"],
    "postgres": ["sqlalchemy>=1.4.29,<2", "psycopg2==2.9.1"],
}

extra_dependencies["all"] = list(set(sum(extra_dependencies.values(), [])))
extra_dependencies["dev"] = extra_dependencies["all"] + [
    # Docs
    "alabaster",
    "sphinx==4.1.1",
    "sphinxcontrib-napoleon",
    "sphinxcontrib-versioning",
    "sphinx-copybutton",
    # Linting
    "flake8",
    "flake8-bugbear",
    "flake8-quotes",
    "isort",
    "black==21.7b0",
    "mypy>=0.930",
    "sqlalchemy[mypy]",
    "types-redis",
    "types-python-dateutil",
    # Misc
    "pre-commit",
    "bumpversion",
    "hiredis",
    "twine",
    # Testing
    "pytest",
    "pytest-benchmark[histogram]",
    "pytest-cov",
    "pytest-timeout",
    "tox",
    "freezegun",
]

setup(
    name="remoulade",
    version=version,
    author="Wiremind",
    author_email="dev@wiremind.io",
    description="Background Processing for Python 3.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=[
        "remoulade",
        "remoulade.brokers",
        "remoulade.cancel",
        "remoulade.cancel.backends",
        "remoulade.middleware",
        "remoulade.rate_limits",
        "remoulade.rate_limits.backends",
        "remoulade.results",
        "remoulade.results.backends",
        "remoulade.scheduler",
        "remoulade.state",
        "remoulade.state.backends",
        "remoulade.cli",
        "remoulade.helpers",
        "remoulade.api",
    ],
    package_data={"remoulade": ["py.typed"]},
    include_package_data=True,
    install_requires=dependencies,
    python_requires=">=3.7",
    extras_require=extra_dependencies,
    entry_points={
        "console_scripts": [
            "remoulade = remoulade.__main__:main",
            "remoulade-ls = remoulade.cli.remoulade_ls:main",
            "remoulade-run = remoulade.cli.remoulade_run:main",
            "remoulade-scheduler = remoulade.cli.remoulade_scheduler:main",
        ]
    },
    scripts=["bin/remoulade-gevent"],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
    ],
)
