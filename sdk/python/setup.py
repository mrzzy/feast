# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os
import re
import subprocess

from setuptools import find_packages

try:
    from setuptools import setup
    from setuptools.command.install import install
    from setuptools.command.develop import develop
    from setuptools.command.egg_info import egg_info
    from setuptools.command.sdist import sdist

except ImportError:
    from distutils.core import setup
    from distutils.command.install import install

NAME = "feast"
DESCRIPTION = "Python SDK for Feast"
URL = "https://github.com/feast-dev/feast"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.6.0"

REQUIRED = [
    "Click==7.*",
    "google-api-core>=1.23.0",
    "google-cloud-bigquery>=2.0.*",
    "google-cloud-bigquery-storage >= 2.0.0",
    "google-cloud-storage>=1.20.*",
    "google-cloud-core==1.4.*",
    "googleapis-common-protos==1.52.*",
    "grpcio==1.31.0",
    "Jinja2==2.11.3",
    "pandas~=1.0.0",
    "pandavro==1.5.*",
    "protobuf>=3.10",
    "PyYAML==5.3.*",
    "fastavro>=0.22.11,<0.23",
    "tabulate==0.8.*",
    "toml==0.10.*",
    "tqdm==4.*",
    "pyarrow==2.0.0",
    "numpy<1.20.0",
    "google",
    "bindr",
    "mmh3",
    "jsonschema",
]

# README file from Feast repo root directory
repo_root = (
    subprocess.Popen(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE)
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
)
README_FILE = os.path.join(repo_root, "README.md")
with open(os.path.join(README_FILE), "r") as f:
    LONG_DESCRIPTION = f.read()

# Add Support for parsing tags that have a prefix containing '/' (ie 'sdk/go') to setuptools_scm.
# Regex modified from default tag regex in:
# https://github.com/pypa/setuptools_scm/blob/2a1b46d38fb2b8aeac09853e660bcd0d7c1bc7be/src/setuptools_scm/config.py#L9
TAG_REGEX = re.compile(
    r"^(?:[\/\w-]+)?(?P<version>[vV]?\d+(?:\.\d+){0,2}[^\+]*)(?:\+.*)?$"
)

proto_dirs = [x for x in os.listdir(repo_root + "/protos/feast")]
proto_dirs.remove("third_party")


def pre_install_build():
    subprocess.check_call("make compile-protos-python", shell=True, cwd=f"{repo_root}")
    subprocess.check_call("make build-sphinx", shell=True, cwd=f"{repo_root}")


class CustomInstallCommand(install):
    def do_egg_install(self):
        pre_install_build()
        install.do_egg_install(self)


class CustomDevelopCommand(develop):
    def run(self):
        pre_install_build()
        develop.run(self)


class CustomEggInfoCommand(egg_info):
    def run(self):
        pre_install_build()
        egg_info.run(self)


setup(
    name=NAME,
    author=AUTHOR,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)) + ['.'],
    install_requires=REQUIRED,
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    extras_require={
        "dev": ["mypy-protobuf==1.*", "grpcio-testing==1.*"],
        "validation": ["great_expectations==0.13.2", "pyspark==3.0.1"],
        "docs": ["grpcio-tools"],
    },
    include_package_data=True,
    license="Apache",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    entry_points={"console_scripts": ["feast=feast.cli:cli"]},
    use_scm_version={"root": "../..", "relative_to": __file__, "tag_regex": TAG_REGEX},
    setup_requires=["setuptools_scm", "grpcio-tools", "mypy-protobuf", "sphinx"],
    package_data={
        "": [
            "protos/feast/**/*.proto",
            "protos/feast/third_party/grpc/health/v1/*.proto",
            "protos/tensorflow_metadata/proto/v0/*.proto",
            "feast/protos/feast/**/*.py",
            "tensorflow_metadata/proto/v0/*.py"
        ],
    },
    cmdclass={
        "install": CustomInstallCommand,
        "develop": CustomDevelopCommand,
        "egg_info": CustomEggInfoCommand,
    },
)
