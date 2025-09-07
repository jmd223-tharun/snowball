# run_dbt.py
"""
    Main operating python file for generating formatted compiled version of snowball project via dbt
    and packaging it as per the requirements.
    It also generates pyspark notebooks from the compiled SQL files.
"""
""" Import necessary libraries """
import os
import re
import sys
import json
import zipfile
import shutil
import subprocess
from .config import *
from git import Repo
from pathlib import Path
from datetime import datetime
from dbt.cli.main import dbtRunner
import nbformat as nbf
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

# === Set OS path & environment variables === #
os.environ["DBT_PROFILES_DIR"] = profiles_dir

compiled_dir  = os.path.join(project_dir, "target", "compiled")
output_zip    = os.path.join(output_dir, "compiled_models.zip")
dbt_seed_dir  = os.path.join(project_dir, "seeds")
notebooks_dir = os.path.join(output_dir, "notebooks")

project_root = project_dir
os.chdir(project_root)

def cleanup_previous_run():
    """Clean up previous compiled files and notebooks"""
    for dir_path in [compiled_dir, notebooks_dir]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"🧹 Cleaned up: {dir_path}")

def run_dbt_deps():
    """Run dbt deps to install dependencies"""
    deps_args = [
        "deps",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir
    ]
    dbt = dbtRunner()
    return dbt.invoke(deps_args)

def run_dbt():
    """Run all dbt models"""
    run_args = [
        "run",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir
    ]
    dbt = dbtRunner()
    return dbt.invoke(run_args)

def run_pre_run_setup():
    """Run the pre_run_setup macro"""
    macro_args = [
        "run-operation",
        "pre_run_setup",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir
    ]
    dbt = dbtRunner()
    return dbt.invoke(macro_args)

def build_dbt_compile_args():
    """Build arguments for dbt compile"""
    return [
        "compile",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir
    ]

def run_dbt_args(cli_args):
    """Run dbt with given arguments"""
    dbt = dbtRunner()
    return dbt.invoke(cli_args)

def zip_directory(source_dir, zip_path):
    """Zip the contents of an entire directory"""
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)

def run_sqlfluff_on_directory(directory_path):
    """
    Run SQLFluff fix on all SQL files in a directory.
    """
    try:
        print(f"🔧 Applying SQLFluff to directory: {directory_path}")
        
        # Run SQLFluff on each SQL file in the directory
        sql_files = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
        
        success_count = 0
        for sql_file in sql_files:
            try:
                # Run SQLFluff on individual file
                result = subprocess.run(
                    ["sqlfluff", "fix", "--force", sql_file],
                    check=False,
                    text=True,
                    capture_output=True,
                    cwd=project_root
                )

                if result.returncode == 0:
                    print(f"✅ SQLFluff completed for: {os.path.basename(sql_file)}")
                    success_count += 1
                else:
                    print(f"⚠️ SQLFluff encountered issues for: {os.path.basename(sql_file)}")
                    if result.stdout:
                        print(f"STDOUT: {result.stdout}")
                    if result.stderr:
                        print(f"STDERR: {result.stderr}")
                        
            except Exception as e:
                print(f"❌ Error processing {os.path.basename(sql_file)}: {e}")
        
        print(f"📊 SQLFluff processed {success_count}/{len(sql_files)} files successfully")
        return success_count > 0
            
    except Exception as e:
        print(f"❌ Unexpected error with SQLFluff for {directory_path}: {e}")
        return False

def apply_sqlfluff_to_compiled():
    """
    Apply SQLFluff to all compiled SQL files before packaging.
    Run SQLFluff on the entire compiled models directory at once for efficiency.
    """
    # check SQLFluff is availability
    try:
        subprocess.run(["sqlfluff", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ SQLFluff is not installed or not available in PATH")
        return False
    
    # Run SQLFluff on the compiled models directory
    return run_sqlfluff_on_directory(compiled_dir)

def generate_notebooks():
    """Generate Jupyter notebooks from compiled SQL by model folder"""
    try:
        os.makedirs(notebooks_dir, exist_ok=True)

        model_folders = set()
        for root, _, files in os.walk(compiled_dir):
            for file in files:
                if file.endswith('.sql'):
                    rel_path = os.path.relpath(root, compiled_dir)
                    if rel_path != '.':
                        folder_name = rel_path.split(os.sep)[-1]
                        if folder_name != "models":
                            model_folders.add(folder_name)

        for folder in model_folders:
            if folder == "models":
                continue
                
            notebook_path = os.path.join(notebooks_dir, f"{folder}_nb.ipynb")
            nb = new_notebook()
            
            folder_name = folder.upper().split('_')[-1]
            nb.cells.append(new_markdown_cell(
                "## SNOWBALL Spark SQL version\n"
                f"#### **Notebook to create {folder_name} layer**\n"
                f"##### **Creating {folder_name} schema to create required {folder_name} tables**\n"
            ))
            nb.cells.append(new_code_cell(f"%%sql\nCREATE SCHEMA IF NOT EXISTS {folder.split('_')[-1]};"))

            if folder == 'tests':
                folder_path = os.path.join(compiled_dir, 'Snowball_dbt', folder)
            else:            
                folder_path = os.path.join(compiled_dir, 'Snowball_dbt', 'models', folder)

            for root, _, files in os.walk(folder_path):
                for file in sorted(files):
                    if file.endswith('.sql'):
                        file_path = os.path.join(root, file)
                        model_name = os.path.splitext(file)[0]
                        
                        with open(file_path, 'r', encoding='utf-8') as f:
                            sql_content = f.read()                    
                        nb.cells.append(new_markdown_cell(f"##### **{model_name}**"))
                        nb.cells.append(new_code_cell(
                            f"%%sql\n"
                            f"DROP TABLE IF EXISTS {folder.split('_')[-1]}.{model_name};\n"
                            f"CREATE TABLE {folder.split('_')[-1]}.{model_name} AS\n"
                            f"{sql_content}"
                        ))
            with open(notebook_path, 'w', encoding='utf-8') as f:
                nbf.write(nb, f)
            print(f"📓 Generated: {os.path.basename(notebook_path)}") 
    except Exception as e:
        print(f"❌ Failed to generate notebooks: {e}")
        return False
    return True

def copy_seed_file(seed_path, target_dir):
    """
    Copy the seed file to target directory and ensure it's named column_mapping.csv
    """
    os.makedirs(target_dir, exist_ok=True)
    target_file = os.path.join(target_dir, "column_mapping.csv")

    try:
        # Check if source and target are the same file
        if os.path.abspath(seed_path) == os.path.abspath(target_file):
            print(f"✅ Source file is already in target location: {os.path.basename(seed_path)}")
        else:
            # Copy the file (will overwrite if exists)
            shutil.copy(seed_path, target_file)
            print(f"✅ Copied: {os.path.basename(seed_path)} -> {os.path.basename(target_file)}")
        
        # Run dbt seed to load the seed file
        run_dbt_args(["seed", "--select", "column_mapping"])
        print(f"✅ Processed mapping file")
        return True
        
    except FileNotFoundError:
        print(f"❌ Seed file not found at: {seed_path}")
        return False
    except Exception as e:
        print(f"❌ Failed to copy seed file: {e}")
        return False

def transform_compiled_sql(sql_file_path):
    """Post-process a compiled SQL file to wrap in stored procedure format."""
    try:
        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_code = f.read()

        # Extract folder inside models and model name
        rel_path = os.path.relpath(sql_file_path, compiled_dir)
        parts = rel_path.split(os.sep)

        # Find index of "models" in path
        if "models" in parts:
            models_idx = parts.index("models")
            if models_idx + 1 < len(parts):
                folder_raw = parts[models_idx + 1]
                model_folder_name = folder_raw.split("_", 1)[1] if "_" in folder_raw else folder_raw
            else:
                print(f"⚠️ Could not determine model folder for {sql_file_path}")
                return
        else:
            print(f"⚠️ Skipping file (no 'models' in path): {sql_file_path}")
            return

        model_name = os.path.splitext(parts[-1])[0]

        # === 1. Add schema + procedure header ===
        proc_header = (
            f"CREATE OR ALTER PROCEDURE {model_folder_name}.sp_{model_name}\n"
            f"AS\nBEGIN\n    SET NOCOUNT ON;\n\n"
            f"    BEGIN\n        DROP TABLE IF EXISTS {model_folder_name}.{model_name}; \n    END;\n\n"
        )

        # === 2. Add INTO clause before last FROM ===
        match = list(re.finditer(r"\bFROM\b", sql_code, re.IGNORECASE))
        if match:
            last_from = match[-1]
            insert_pos = last_from.start()
            sql_code = sql_code[:insert_pos] + f"INTO {model_folder_name}.{model_name}\n" + sql_code[insert_pos:]

        # === 3. Add END ===
        sql_code = proc_header + sql_code.strip() + "\nEND;"

        # Overwrite the file
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(sql_code)

        print(f"✅ Transformed: {sql_file_path}")

    except Exception as e:
        print(f"❌ Failed to transform {sql_file_path}: {e}")

def process_compiled_sql_files():
    """Walk through compiled models directory and transform all SQL files."""
    for root, _, files in os.walk(compiled_dir):
        for file in files:
            if file.endswith(".sql"):
                transform_compiled_sql(os.path.join(root, file))

def clone_repo(git_url: str) -> str:
    """
    Clone the Git repository from `git_url` into the current directory.
    
    Args:
        git_url (str): URL of the git repository to clone.
        
    """
    repo_name = os.path.splitext(os.path.basename(git_url))[0]
    clone_path = os.path.join(os.getcwd(), repo_name)
    
    # If folder exists, skip cloning to avoid overwriting
    if os.path.exists(clone_path):
        print(f"Repository already cloned at {clone_path}")
    else:
        print(f"Cloning into {clone_path} ...")
        Repo.clone_from(git_url, clone_path)
        print("Clone completed.")
    return(os.path.join(clone_path, 'seeds', 'column_mapping.csv'))

def copy_csv_to_downloads(src_csv_path: str) -> str:
    """
    Copy a CSV file from src_csv_path to the Downloads folder of the current user.
    
    Args:
        src_csv_path (str): The source path of the CSV file to copy.
        
    Returns:
        str: The full path to the copied file in the Downloads folder.
    """
    # Get the user's Downloads folder path dynamically
    downloads_dir = Path.home() / "Downloads"
    
    # Ensure Downloads folder exists (usually it does)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    
    src_path = Path(src_csv_path)
    if not src_path.is_file():
        raise FileNotFoundError(f"Source CSV file not found: {src_csv_path}")
    
    # Destination path keeps the same filename
    dest_path = downloads_dir / src_path.name
    
    # Copy file
    shutil.copy2(src_path, dest_path)

def main():
    # Clone the latest repo from Snowball dbt
    mapping_file_path = clone_repo("https://github.com/jmangroup/snowball_dbt.git")
    print(mapping_file_path)
    copy_csv_to_downloads(mapping_file_path)

    # Clean up previous runs
    cleanup_previous_run()

    if not os.path.exists(mapping_file):
        print(f"❌ Mapping file not found at the specified path: {mapping_file}")
        sys.exit(1)

    copy_seed_file(mapping_file, dbt_seed_dir)
    print(f"\nColumn mapping file has been downloaded to {Path.home()}/Downloads/column_mapping.csv, Please update it & add profiles.yml file and continue...")
    print("\nWhat would you like to do?")
    print("1: Package the full dbt project")
    print("2: Compile the SQL project code")
    print("3: Get the pyspark notebooks for compiled SQL")

    try:
        user_choice = int(input("Enter your choice (1, 2 or 3): ").strip())
    except ValueError:
        print("❌ Invalid input. Please enter 1, 2 or 3.")
        return

    if user_choice == 1:
        try:
            print("📦 Packaging the full dbt project...")
            zip_directory(project_dir, output_zip)
            print("\n✅ Your full dbt project has been packaged successfully!")
            print(f"📦 Zipped dbt project saved at: {output_zip}")
        except Exception as e:
            print(f"❌ Failed to zip dbt project: {e}")

    elif user_choice == 2:
        print("📦 Installing dbt dependencies...")
        deps_result = run_dbt_deps()
        if not deps_result.success:
            print("❌ dbt deps failed")
            return
        
        try:
            print("🚀 Running pre-run setup macro...")
            macro_result = run_pre_run_setup()
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            print("🚀 Running dbt models...")
            run_result = run_dbt()
            if not run_result.success:
                print("❌ dbt run failed")
                return
            print("✅ dbt run completed successfully!")
        except Exception as e:
            print(f"❌ dbt run failed: {e}")
            return
        
        print("🔨 Compiling dbt models...")
        compile_args = build_dbt_compile_args()
        compile_result = run_dbt_args(compile_args)

        if compile_result.success:
            print("✅ dbt compile completed successfully!")
            print("✨ Applying SQLFluff rules...")
            sqlfluff_success = apply_sqlfluff_to_compiled()
            if sqlfluff_success:
                print("📊 SQLFluff formatting completed successfully!")
            else:
                print("⚠️ SQLFluff encountered some issues, but continuing with processing...")
            process_compiled_sql_files()
            zip_directory(compiled_dir, output_zip)
            print(f"📦 Compiled & formatted SQL files zipped at: {output_zip}")
        else:
            print("❌ dbt compile failed")

    elif user_choice == 3:
        print("📦 Installing dbt dependencies...")
        deps_result = run_dbt_deps()
        if not deps_result.success:
            print("❌ dbt deps failed")
            return
            
        try:
            print("🚀 Running pre-run setup macro...")
            macro_result = run_pre_run_setup()
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            print("🚀 Running dbt models...")
            run_result = run_dbt()
            if not run_result.success:
                print("❌ dbt run failed")
                return
            print("✅ dbt run completed successfully!")
        except Exception as e:
            print(f"❌ dbt run failed: {e}")
            return

        print("🔨 Compiling dbt models...")        
        compile_args = build_dbt_compile_args()
        result = run_dbt_args(compile_args)

        if result and result.success:
            print("✨ Applying SQLFluff rules...")
            sqlfluff_success = apply_sqlfluff_to_compiled()
            if sqlfluff_success:
                print("📊 SQLFluff formatting completed successfully!")
            else:
                print("⚠️ SQLFluff encountered some issues, but continuing with processing...")

            print("\n🔨 Generating PySpark notebooks...")
            generate_notebooks()
            print(f"\n📓 Notebooks saved to: {notebooks_dir}")

            if os.path.exists(output_zip):
                os.remove(output_zip)
            zip_directory(notebooks_dir, output_zip)
            print(f"📦 Notebooks zipped at: {output_zip}")

        else:
            print("❌ dbt compile failed")

    else:
        print("❌ Invalid choice. Please enter either 1, 2 or 3.")

if __name__ == "__main__":
    main()