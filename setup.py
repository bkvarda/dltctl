import sys
from setuptools import setup, find_packages

if sys.version_info < (3,5):
    sys.exit('Python < 3.5 is not supported')

version = "0.1"
setup(
    name='dltctl',
    version=version,
    packages=find_packages(include=['dltctl*']),
    install_requires=[
        # Note: please keep this in sync with `requirements.txt`.
        'databricks-cli>=0.17.3',
        'databricks-sql-connector',
        'click>=7.0',
        'pyjwt>=1.7.0',
        'oauthlib>=3.1.0',
        'requests>=2.17.3',
        'tabulate>=0.7.7',
        'six>=1.10.0',
        'configparser>=0.3.5;python_version < "3.6"',
        'pandas',
        'pyarrow',
        'tabulate'
    ],
    entry_points='''
        [console_scripts]
        dltctl=dltctl.cli:cli
    ''',
    zip_safe=False,
    author='Brandon Kvarda',
    author_email='brandon@databricks.com',
    description='A command line interface for Databricks Delta Live Tables',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords='databricks dlt delta live tables',
    url='https://github.com/databrickslabs/dltctl'
)