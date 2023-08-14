# -*- coding: utf-8 -*-
import os

import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    required = f.read().splitlines()

setuptools.setup(
    name="pyflake-client",
    version=os.getenv("PYFLAKE_SEMVER", "0.0.1"),
    author="Tobias Antonsen",
    author_email="tobias@tsant.no",
    description="A client written in Python for testing integrations manipulating DDL in Snowflake.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Tsanton/pyflake-client",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=required,
    packages=setuptools.find_packages(exclude=["*tests*"]),
    python_requires=">=3.8",
)
