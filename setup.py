from setuptools import setup, find_packages

VERSION = "0.1.dev0"

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="neopylib",
    version=VERSION,
    author="Clément Trassoudaine",
    author_email="clement.trassoudaine@eurecom.fr",
    description="A python toolkit for importing datasets into neo4j",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="neo4j cypher graph",
    url="https://github.com/intv0id/neopyimport",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.6.6",
    install_requires=["pandas"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

