# dbt_runner

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)]

## Overview

`dbt_runner` is a Python package for automating the compilation, formatting, and packaging of dbt projects. It also generates PySpark notebooks from compiled SQL files. This tool streamlines Snowball ARR-related dbt workflows.

## Features

- Install dependencies and run dbt models programmatically
- Compile dbt projects and apply SQLFluff formatting
- Transform compiled SQL files into stored procedure format
- Generate PySpark notebooks for compiled SQL models
- Package the entire project as a zipped archive for distribution

## Installation

Install directly from your organization's private Git repository:


## Usage

After installation, run the CLI tool:


Follow the interactive prompts to:
- Package the full dbt project
- Compile the SQL project code
- Generate PySpark notebooks from compiled SQL

Alternatively, import `run_dbt` module in your Python code:


## Requirements

- Python 3.7 or newer
- Dependencies installed automatically via `pip` (dbt-core, sqlfluff, nbformat, pyspark)

## Contributing

Contributions are welcome! Please fork the repository and create a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or issues, contact Vishal Verma <kondurutharun@jmangroup.com>.

