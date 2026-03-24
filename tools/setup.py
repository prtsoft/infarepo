from setuptools import setup, find_packages

_BASE_REQUIRES = [
    "click>=8.1",
    "lxml>=4.9",
    "rich>=13.0",
    "pyyaml>=6.0",
]

setup(
    name="migration-tools",
    version="0.1.0",
    packages=find_packages(),
    install_requires=_BASE_REQUIRES,
    extras_require={
        # Install everything needed for local development and testing
        "dev": [
            "pytest>=7.4",
            "pytest-cov>=4.0",
            "ruff>=0.4",
            "openpyxl>=3.1",
            "jinja2>=3.1",
        ],
        # Drivers for connecting to source / target databases
        "sql-server": ["pyodbc>=4.0"],
        "databricks": ["databricks-sql-connector>=3.0"],
        # PySpark + Delta for local testing / script validation
        "spark": ["pyspark>=3.3", "delta-spark>=2.3"],
        # AWS SDK for SSM / S3 integration
        "aws": ["boto3>=1.26"],
        # Excel support for S2T export and EXCEL source reads
        "excel": ["openpyxl>=3.1"],
        # HTML sign-off reports
        "reports": ["jinja2>=3.1"],
        # Everything for a migration project engineer
        "all": [
            "pyodbc>=4.0",
            "databricks-sql-connector>=3.0",
            "openpyxl>=3.1",
            "jinja2>=3.1",
            "boto3>=1.26",
        ],
    },
    entry_points={
        "console_scripts": [
            "pc-extractor=pc_extractor.cli:main",
            "glue-gen=glue_gen.cli:cli",
            "databricks-gen=databricks_gen.cli:cli",
            "param-translator=param_translator.cli:cli",
            "validation-harness=validation_harness.cli:cli",
            "workflow-gen=workflow_gen.cli:cli",
            "review-gen=review_gen.cli:cli",
        ],
    },
    python_requires=">=3.10",
)
