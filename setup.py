import sys
from setuptools import setup, find_packages

if sys.version_info < (3,5):
    sys.exit('Python < 3.5 is not supported')

def get_version():
    version = None
    with open('dltctl/version.py', 'r') as f:
        val = (f.readline()
        .split('=')[1]
        .strip()
        .strip('/"'))
        version = val
    return version


__version__ = get_version()

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='dltctl',
    version=__version__,
    packages=find_packages(include=['dltctl*']),
    install_requires=[
        'databricks-cli>=0.17.3',
        'colorama>=0.4.5',
        'PyYAML==6.0',
    ],
    entry_points='''
        [console_scripts]
        dltctl=dltctl.cli:cli
    ''',
    zip_safe=False,
    author='Brandon Kvarda',
    author_email='brandon@databricks.com',
    description='A command line interface for Databricks Delta Live Tables',
    long_description=readme(),
    long_description_content_type='text/markdown',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords='databricks dlt delta live tables',
    url='https://github.com/bkvarda/dltctl'
)