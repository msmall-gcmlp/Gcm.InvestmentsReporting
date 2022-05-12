from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as version_file:
    version = version_file.read().strip()

import os

base_src = "src"
project = "gcm"


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        if "pycache" not in path and "egg-info" not in path:
            paths.append(
                path.replace(f"{base_src}/", "").replace("\\", ".")
            )
    return paths


extra_paths = package_files(f"{base_src}/{project}")

setup(
    name="gcm-investmentsreporting",
    version=version,
    description="Investments Team report generation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=extra_paths,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "blessings ~= 1.7",
        "gcm-programrunner",
        "gcm-dao",
        "pandas",
        "datetime",
        "openpyxl",
        "pillow",
        "gcm-investmentsdataprovider==0.4.0.dev2205120003",
        "gcm-investmentsquantlib==0.37.0.dev2205121249",
        "PyPDF2"
    ],
    extras_require={
        "dev": ["pytest>=3.7", "tox>=3.23", "wheel"],
    },
    url="https://github.com/GCMGrosvenor/Gcm.InvestmentsReporting",
    author="Nobody",
    author_email="nobody@gcmlp.com",
)
