import setuptools
from oedi import __version__


with open("README.md") as fp:
    long_description = fp.read()


def read_lines(filename):
    with open(filename, "r") as f:
        return f.readlines()


setuptools.setup(
    name="oedi",
    version=__version__,
    description="The data access tool for constructing Open Energy Data Initiative (OEDI) data lake.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="NREL",
    url="https://github.com/openEDI/open-data-access-tools",
    packages=setuptools.find_packages(),
    install_requires=read_lines("requirements.txt"),
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 0 - Beta",
        "Intended Audience :: Developers",
        "License :: Apache License, Version 2.0.",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Topic :: Data Lake :: Open Energy Data Initiative",
        "Topic :: Utilities"
    ],
    entry_points={"console_scripts": ["oedi = oedi.cli.oedi:cli"]},
    test_suite="tests"
)
