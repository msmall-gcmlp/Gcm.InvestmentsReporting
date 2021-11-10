from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as version_file:
    version = version_file.read().strip()

setup(
    name='gcm-investmentsreporting',
    version=version,
    description='Investments Team report generation',
    long_description=long_description,
    long_description_content_type="text/markdown",
    py_modules=["gcm.investmentsreporting"],
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "blessings ~= 1.7",
        "gcm-dao"
    ],
    extras_require={
        "dev": [
            "pytest>=3.7",
            "tox>=3.23",
            "wheel"
        ],
    },
    url="https://github.com/GCMGrosvenor/Gcm.InvestmentsReporting",
    author="Anna Galstyan",
    author_email="agalstyan@gcmlp.com",
)
