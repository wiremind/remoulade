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


dependencies = ["prometheus-client>=0.2", "pytz", "python-dateutil>=2.8.0", "typing-extensions>=3.8", "attrs>=19.2.0"]

extra_dependencies = {
    "rabbitmq": ["amqpstorm>=2.6,<3"],
    "redis": ["redis~=5.0"],
    "server": ["flask>=1.1,~=2.3.3", "marshmallow>=3,<4", "flask-apispec"],
    "postgres": ["sqlalchemy>=1.4.29,<2", "psycopg2==2.9.5"],
    "pydantic": ["pydantic>=2.0", "simplejson"],
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
    "black~=23.12",
    "mypy~=1.10.0",
    "pyupgrade~=3.15.0",
    "sqlalchemy[mypy]",
    "types-redis",
    "types-python-dateutil",
    "types-simplejson",
    "types-pytz",
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
    "pytest-asyncio",
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
    python_requires=">=3.8",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
    ],
)
