from setuptools import setup

install_requirements = [
    'click',
    'psycopg2',
    'requests',
    'parsimonious'
]

tests_requirements = [
    'pytest',
]

setup_requirements = [
    'pytest-runner'
]

setup(
    name="splitgraph-prototype",
    version="0.0",
    packages=['splitgraph'],
    entry_points={
        'console_scripts': ['sg=splitgraph.commandline:cli'],
    },
    install_requires=install_requirements + tests_requirements,
    tests_require=tests_requirements,
    setup_requires=setup_requirements,
    test_suite='test/splitgraph'
)
