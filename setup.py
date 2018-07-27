from setuptools import setup

setup(
    name="splitgraph-prototype",
    version="0.0",
    packages=['splitgraph'],
    entry_points={
        'console_scripts': ['sg=splitgraph.commandline:cli'],
    }, install_requires=['click', 'psycopg2']
)