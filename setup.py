"""
TScrape - Modern Telegram Channel Scraper
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="tscrape",
    version="1.0.0",
    author="TScrape",
    description="Modern Telegram Channel Scraper combining best practices from 2026",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tscrape/tscrape",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "telethon>=1.34.0",
        "pyarrow>=15.0.0",
        "pandas>=2.0.0",
        "click>=8.1.0",
        "rich>=13.0.0",
    ],
    extras_require={
        "fast": ["cryptg>=0.4.0"],
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "tscrape=tscrape.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Communications :: Chat",
        "Topic :: Internet",
    ],
    keywords="telegram scraper telethon osint data-collection",
)
