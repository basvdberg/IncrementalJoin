from setuptools import setup, find_packages

setup(
    name='incremental-join',
    version='0.1.0',
    author='Bas van den Berg',
    author_email='b.vdberg@c2h.nl',
    description='This python library helps you to join 2 huge incrementally refreshed tables. Major benefits: performance, efficiency and consistency.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/basvdberg/IncrementalJoin',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        # List your library's dependencies here
    ],
)