from setuptools import setup, find_packages
import os


this_directory = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""

setup(
    name="snowball",      
    version="1.0.15",                            # Package version
    description="Generate compiled dbt projects and PySpark notebooks.",
    long_description=long_description,          # Long description (usually README content)
    long_description_content_type="text/markdown",
    author="Vishal Verma",                         # Author name
    author_email="vishal.verma@jmangroup.com",
    url="https://gitserver/org/dbt_runner",    # URL of project homepage or repo
    packages=find_packages(),                   # Automatically find packages inside your project directory
    install_requires=[       
        "GitPython>=3.1.0",                   # List dependencies your package requires
        "dbt-core",
        "sqlfluff",
        "nbformat",
        "pyspark",
        "dbt-sqlserver==1.9.0",
        "dbt-fabric",
        "dbt-snowflake",
        "dbt-databricks"

    ],
    entry_points={                              # Makes a console command available after install
        'console_scripts': [
            'run-dbt=dbt_runner.run_dbt:main',
        ],
    },
    classifiers=[                               
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',                    # Minimum Python version required
    include_package_data=True,                  
)
