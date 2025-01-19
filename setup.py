#!/usr/bin/env python3
"""
Setup configuration for csv_to_rds package
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="csv_to_rds",
    version="0.1.0",
    author="Yogesh Chaudhari",
    author_email="yogesh@cyogesh.com",
    description="A tool for loading CSV files into AWS RDS databases",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yogeshc/csv_to_rds",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Migration",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.0.0",
        "sqlalchemy>=1.4.0",
        "pymysql>=1.0.0",
        "boto3>=1.26.0",
        "configparser>=5.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.900",
        ],
    },
    entry_points={
        "console_scripts": [
            "csv-to-rds=csv_to_rds.main:main",
        ],
    },
)