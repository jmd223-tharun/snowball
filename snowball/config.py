"""
config.py

    This configuration file contains all your static values required for the Snowball ARR project.
    You can define constants such as project paths, environment settings, update mapping csv, platform / adapter
    and other parameters that are reused across the project.

"""
import yaml
from pathlib import Path

# === Project paths === #
profiles_dir = str(Path.home() / ".dbt")
project_dir  = str(Path.home() / "Downloads/snowball_dbt")
mapping_file = str(Path.home() / "Downloads/column_mapping.csv")
output_dir   = str(Path.home() / "Downloads")

def load_dbt_profile(profile_name: str = "Snowball_dbt", target: str = "dev") -> dict:
    """
    Load the DBT profile credentials from ~/.dbt/profiles.yml
    
    Args:
        profile_name (str): The name of the DBT profile (default: "Snowball_dbt")
        target (str): The target environment inside the profile (default: "dev")
        
    Returns:
        dict: Dictionary containing database connection parameters such as:
              platform, database, schema, user, password, account, role, warehouse, threads
              
    Raises:
        FileNotFoundError: If profiles.yml does not exist in ~/.dbt/
        ValueError: If the specified profile or target is not found
    """
    profiles_path = Path.home() / ".dbt" / "profiles.yml"

    try:
        with open(profiles_path, 'r') as file:
            profiles = yaml.safe_load(file)

        profile = profiles.get(profile_name)
        if not profile:
            raise ValueError(f"Profile '{profile_name}' not found in profiles.yml")

        outputs = profile.get("outputs", {})
        target_profile = outputs.get(target)
        if not target_profile:
            raise ValueError(f"Target '{target}' not found under profile '{profile_name}' in profiles.yml")

        db_vars = {
            "platform" : target_profile.get("type", "snowflake"),
            "database" : target_profile.get("database", ""),
            "schema"   : target_profile.get("schema", ""),
            "user"     : target_profile.get("user", ""),
            "password" : target_profile.get("password", ""),
            "account"  : target_profile.get("account", ""),
            "role"     : target_profile.get("role", ""),
            "warehouse": target_profile.get("warehouse", ""),
            "type"     : target_profile.get("type", ""),
            "threads"  : target_profile.get("threads", 1)
        }

        return db_vars

    except FileNotFoundError:
        raise FileNotFoundError(f"profiles.yml not found at {profiles_path}")
    except Exception as e:
        raise Exception(f"Error loading DBT profile: {e}")


# Load DB credentials once on import
try:
    db_vars = load_dbt_profile()
except Exception as err:
    # If you want, you can handle missing profiles gracefully here,
    # for example, set db_vars = {} or print a warning.
    # For now, we raise the error.
    raise err  

# === Steps to proceed === #
"""
    1 -> Update the given referencing file as per your's tables fields name
    2 -> Run : python run_dbt.py
""" 