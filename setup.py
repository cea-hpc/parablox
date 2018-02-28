# Copyright (C) 2018  quentin.bouget@cea.fr
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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

    license='LGPL-3.0+',

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
