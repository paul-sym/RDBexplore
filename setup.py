import setuptools

with open("README.md", 'r') as fh:
	long_description = fh.read()

setuptools.setup(
	name='rdbexplore',
	version='0.0.dev1',
	author='P. Symmers',
	author_email='paul_symmers@hotmail.com',
	description='A python package using graph constructions to help explore and analyse enterprise scale relational databases.',
	long_description=long_description,
	long_description_content_type="text/markdown",
	url="https://github.com/paul-sym/rdbexplore",
	packages=setuptools.find_packages(),
	classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
	)