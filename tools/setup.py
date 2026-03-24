from setuptools import setup, find_packages

setup(
    name="pc-extractor",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click>=8.1",
        "lxml>=4.9",
        "rich>=13.0",
    ],
    entry_points={
        "console_scripts": [
            "pc-extractor=pc_extractor.cli:main",
        ],
    },
    python_requires=">=3.10",
)
