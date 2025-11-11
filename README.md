# Snowball Package

**Snowball** is a centralized, cross-platform package designed to generate **dbt-based code** compatible with multiple data platforms including **SQL Server, Snowflake, Databricks, Fabric, and RedShift**. The package produces both **Spark SQL notebooks** and **plain SQL scripts**, bundling all required libraries and dependencies to integrate with dbt projects and create platform-specific code versions.

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Features](#features)
- [Supported Platforms](#supported-platforms)
- [Project Structure](#project-structure)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Upcoming Enhancements](#upcoming-enhancements)
- [License](#license)
- [Contact](#contact)

## Overview

The **`dbt_runner`** component (part of the Snowball ecosystem) is a **Python-based automation tool** that:

- Connects and runs dbt commands through python
- Compiles and formats dbt projects
- Packages dbt workflows into **ready-to-distribute archives**
- Automates dependency management and platform-specific adaptations

Snowball streamlines analytics, reporting, and R&D workflows by automating code generation and deployment processes across multiple data platforms.

## Requirements

### Prerequisites

- **Python 3.7 or higher**
- **Git** (for package installation)
- **DBT Core** (installed automatically)
- Access to target data platform credentials

### Automatic Dependencies

The following Python packages are installed automatically with Snowball:

- `dbt-core` - Core dbt functionality
- `sqlfluff` - SQL code formatting and linting
- `nbformat` - Jupyter notebook creation and manipulation
- `pyspark` - Spark SQL integration

## Installation

### Basic Installation

Install Snowball directly from the Git repository:

```bash
pip install git+https://github.com/jmangroup/snowball.git
```

### Development Installation

For development contributions, clone and install in editable mode:

```bash
git clone https://github.com/jmangroup/snowball.git
cd snowball
pip install -e .
```

### Verification

Verify installation by checking the available command:

```bash
snowball --help
```

## Quick Start

### Step 1: Initial Setup

1. Install the package as shown above
2. The installation automatically places a `column_mapping.csv` file in your Downloads folder

### Step 2: Launch Snowball

```bash
snowball
```

### Step 3: Interactive Configuration

1. **Select your target platform** from the interactive menu
2. **Choose your version requirements**
3. **Follow the setup prompts** for any additional configuration

### Step 4: Generate Project

Snowball will automatically:
- Compile your dbt project
- Generate platform-specific code
- Create PySpark notebooks (if selected)
- Package everything into a zip file in your Downloads directory

## Configuration

### Column Mapping File

The `column_mapping.csv` file (located in your Downloads folder) maps dimensions for your project:
Update this file as per your dimensions as in below stated example format:

**Example structure:**
```csv
source_column,target_dimension,dimension_type,data_type
user_id,customer,customer,string
order_date,date,NULL,date
item_id,product,product,string
amount,revenue,NULL,float
```

### DBT Profiles Configuration

Configure your `profiles.yml` file to connect to your target data platform:

**File location:**
- Windows: `C:\Users\{UserName}\.dbt\profiles.yml`
- Linux/Mac: `~/.dbt/profiles.yml`

**Example Snowflake configuration:**
```yaml
snowball_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account.region
      user: your_username
      password: your_password
      database: your_database
      schema: your_schema
      warehouse: your_warehouse
      role: your_role
```

**Example Databricks configuration:**
```yaml
snowball_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: your_catalog
      schema: your_schema
      host: your_host.databricks.com
      http_path: your_http_path
      token: your_token
```

## Features

### Core Capabilities

- **Automated Dependency Management** - Installs and manages all required libraries
- **DBT Project Compilation** - Programmatic execution and compilation of dbt models
- **SQL Quality Enforcement** - SQLFluff formatting applied to all compiled SQL
- **Multi-Format Output** - Generates both PySpark notebooks and plain SQL scripts
- **Production-Ready Code** - Transforms dbt outputs into stored procedure-ready code
- **Cross-Platform Compatibility** - Single source to multiple target platforms

### Advanced Features

- **Template-Based Generation** - Customizable code templates for different platforms
- **Error Handling** - Comprehensive error reporting and validation
- **Logging** - Detailed execution logs for debugging
- **Batch Processing** - Handles multiple models efficiently

## Supported Platforms

### Currently Supported

- **SQL Server** (2016+)
- **Snowflake** (All versions)
- **Databricks** (Unity Catalog and legacy)
- **Fabric** (Microsoft Fabric environments)
- **RedShift** (PostgreSQL-compatible versions)

### Output Formats

- **PySpark Notebooks** (`.ipynb`) - For Databricks and Spark environments
- **Project Archives** (`.zip`) - Complete packaged solutions

## Project Structure

### Generated Output Structure

```
snowball_project.zip/
│
├── notebooks/
│   ├── model_1.ipynb
│   ├── model_2.ipynb
│   └── ...
│
├── sql_scripts/
    ├── model_1.sql
    ├── model_2.sql
    └── ...

```

## Usage Examples

### Basic Usage

```bash
# Start the interactive Snowball process
snowball

## Troubleshooting

### Common Issues

**DBT Profile Errors**
```
Error: Could not connect to target database
```
**Solution:** Verify your `profiles.yml` configuration and network connectivity.

**Missing Dependencies**
```
ModuleNotFoundError: No module named 'dbt'
```
**Solution:** Reinstall the package: `pip install --force-reinstall git+https://github.com/jmangroup/snowball.git`

**Column Mapping Issues**
```
Error: Invalid column mapping configuration
```
**Solution:** Check the `column_mapping.csv` file format and required columns.

### Debug Mode

Enable verbose logging for troubleshooting:

```bash
snowball --verbose --log-level DEBUG
```

### Getting Help

Check the generated log files in the output directory or contact support with your error details.

## Contributing

We welcome contributions from the group

## Upcoming Enhancements

### Short-term Roadmap

- **Documentation Support** - Get documentation for SP's/models and test files in downloads along with zipped output
- **Performance Optimizations** - Improved execution speed and memory usage
- **Extended Platform Support** - BigQuery, PostgreSQL, and Oracle integration
- **Enhanced Templates** - More customizable code generation templates
- **Aditional Buckets Calculation** - Add extra buckets from projects

### Long-term Vision

- **Advanced Analytics** - Performance metrics and optimization suggestions
- **Plugin Architecture** - Extensible platform support

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for full details.

**Permissions:**
- Commercial use
- Modification
- Distribution
- Private use

**Limitations:**
- Liability
- Warranty

## Contact

### Technical Support

- **Primary Contact**: Vishal Verma
- **Other Contact**: Konduru Tharun
- **Email**: vishal.verma@jmangroup.com, kondurutharun@jmangroup.com
- **Repository**: https://github.com/jmangroup/snowball

### Issue Reporting

Please report bugs and feature requests through:
- **GitHub Issues**: https://github.com/jmangroup/snowball/issues

### Community

- **Releases**: Check the GitHub releases page for latest versions

---

**Note**: This package is actively maintained. For the latest updates and announcements, please check the GitHub repository regularly.