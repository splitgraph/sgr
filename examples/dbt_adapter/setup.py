#!/usr/bin/env python
from setuptools import find_namespace_packages
from setuptools import setup

package_name = "dbt-splitgraph"
package_version = "0.0.1"
description = """The Splitgraph adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Splitgraph",
    author_email="support@splitgraph.com",
    url="https://www.splitgraph.com",
    packages=find_namespace_packages(),
    package_data={
        "dbt": ["include/splitgraph/macros/*.sql", "include/splitgraph/dbt_project.yml",]
    },
    install_requires=["dbt-core", "splitgraph"],
)
