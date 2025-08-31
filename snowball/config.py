"""
config.py

    This configuration file contains all your static values required for the Snowball ARR project.
    You can define constants such as project paths, environment settings, update mapping csv, platform / adapter
    and other parameters that are reused across the project.

"""

# === Project paths === #
profiles_dir = r"C:\Users\KonduruTharun\.dbt"
project_dir  = r"C:\Works\SnowballDBT\snowball_test\snowball_dbt"
mapping_file = r"C:\Works\SnowballDBT\snowball_test\snowball_dbt\seeds\column_mapping.csv"
output_dir   = r"C:\Works\SnowballDBT\snowball_test"

# === Database configuration === #
db_vars = {
    "platform": "snowflake",
    "database": "snowball_dbt_new_layer_test",
    "schema"  : "dbo",
    "table"   : "sample_arr_dataset"
    # Add other variables if required
}

# === Steps to proceed === #
"""
    1 -> Update the given referencing file as per your's tables fields name
    2 -> Run : python run_dbt.py
"""
