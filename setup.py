from setuptools import setup

install_requirements = [
    'click',
    'psycopg2-binary',
    'requests',
    'parsimonious'
]

tests_requirements = [
    'pytest',
    'pyfakefs',
    'pytest-cov',
    'pytest-env'
]

setup_requirements = [
    'pytest-runner'
]

setup(
    name="splitgraph-prototype",
    version="0.0",
    packages=['splitgraph'],
    entry_points={
        'console_scripts': ['sgr=splitgraph.commandline:cli'],
    },
    install_requires=install_requirements + tests_requirements,
    tests_require=tests_requirements,
    setup_requires=setup_requirements,
    test_suite='test/splitgraph'
)
