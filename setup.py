from setuptools import setup, find_packages

with open("readme.rst", "r", encoding='utf-8') as fh:
    long_description = fh.read()
setup(
    name="file_stream",
    version="0.1.7",
    author="Nutalk",
    author_email="ht2005112@hotmail.com",
    description="process data as stream.",
    license="BSD",
    keywords="file",
    url="https://github.com/nutalk/file_stream",
    packages=find_packages(),
    long_description=long_description,
    install_requires=[
        'mysql-connector-python',
        'retry',
        'kafka',
        'redis'
            ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)

