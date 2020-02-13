from setuptools import setup, find_packages

with open("readme.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()
setup(
    name="file_stream",
    version="0.0.1",
    author="Nutalk",
    author_email="ht2005112@hotmail.com",
    description="process data as stream.",
    license="BSD",
    keywords="file",
    url="https://github.com/nutalk/file_stream",
    packages=find_packages(),
    long_description=long_description,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
