"""Setup do projeto NewsData Bronze Layer."""

from setuptools import setup, find_packages

setup(
    name="newsdata_bronze",
    version="1.0.0",
    description="Bronze Layer - Ingestão de dados da API NewsData.io",
    author="JoeKing",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "pandas>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
        ],
    },
)
