#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="bigquery_shim",
    version="0.5.8",
    packages=["bigquery_shim"],
    install_requires=[
        "google-cloud-bigquery == 1.16.0",
        "google-cloud-storage == 1.22.0",
        "regex",
    ],
)
