# author: Quentin Bouget <quentin.bouget@cea.fr>
#

"""
parablox's setup module
"""

from setuptools import setup, find_packages

import parablox

setup(
    name='parablox',
    version=parablox.__version__,

    description='A dev-friendly multiprocessing framework',

    # The project's main homepage
    url='https://github.com/cea-hpc/parablox',

    # Author details
    author='Quentin Bouget',
    author_email='quentin.bouget@cea.fr',

    license='LGPLv3',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        ],

    keywords='multiprocessing performance lightweight development',

    packages=find_packages(exclude=['tests*']),

    python_requires='~=3.4',
    )
