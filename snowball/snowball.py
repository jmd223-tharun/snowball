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
import msvcrt
import time
from .config import *
from git import Repo
from pathlib import Path
from datetime import datetime
from dbt.cli.main import dbtRunner
import nbformat as nbf
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
from tqdm import tqdm
import threading
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from rich.progress import Progress, BarColumn, TextColumn

# === Set OS path & environment variables === #
os.environ["DBT_PROFILES_DIR"] = profiles_dir

compiled_dir  = os.path.join(project_dir, "target", "compiled")
dbt_seed_dir  = os.path.join(project_dir, "seeds")
notebooks_dir = os.path.join(output_dir, "notebooks")

term_width = shutil.get_terminal_size().columns
bar_width = term_width // 4
############  Formatting Functions ##############

def welcome_message():
    print("\n")
    message = "Welcome to Snowball Product!"
    width = len(message) + 8  # padding for stars
    border = "*" * width

    # Prepare the lines to print
    line1 = border
    line2 = "*" + message.center(width - 2) + "*"
    line3 = border

    # Get terminal width
    term_width = shutil.get_terminal_size().columns

    # Center the output lines relative to terminal width
    print(line1.center(term_width))
    print(line2.center(term_width))
    print(line3.center(term_width))

def rotating_slash_after(text, duration_sec=5, passed=1):
    print(text, end=" ", flush=True)
    spinner = ['|', '/', '-', '\\']
    end_time = time.time() + duration_sec
    i = 0
    while time.time() < end_time:
        sys.stdout.write('\r' + text + " " + spinner[i % len(spinner)])
        sys.stdout.flush()
        time.sleep(0.1)
        i += 1
    green_tick = "\u2714"     # Unicode check mark
    green_color = "\033[92m"  # ANSI green
    red_cross = "\u274C"
    red_color = "\033[91m"
    reset_color = "\033[0m"   # Reset color
    if(passed == 1):
        print_msg = f"{text} {green_color}Done {green_tick}{reset_color}\n"
    else:
        print_msg = f"{text} {red_color}Failed {red_cross}{reset_color}\n"
    sys.stdout.write('\r' + print_msg)
    sys.stdout.flush()

def blinking_dots_input(base_text="Press Enter to continue"):
    dots = ['', '.', '..', '...']
    i = 0
    print(base_text, end='', flush=True)

    while True:
        print('\r' + base_text + dots[i % len(dots)] + '   ', end='', flush=True)
        time.sleep(0.5)
        i += 1

        # Non-blocking key check for Enter press
        if msvcrt.kbhit():
            key = msvcrt.getwch()
            if key == '\r':  # Enter key on Windows
                break

def initial_set_up():
    text = "Setting up initial requirements"
    width = len(text) + 8  # padding for stars
    border = "*" * width

    # Prepare the lines to print
    line1 = border
    line2 = " " + text.center(width - 2)
    line3 = ("*" * (len(line2) // 3)).center(width - 2)
    line4 = "1. Collecting latest repo from Git "
    line5 = f"2. Column mapping file has been downloaded to {Path.home()}/Downloads/column_mapping.csv "
    line6 = "3. Please update Column mapping file as per your revenue data and save it to Downloads"
    line7 = "4. Create a folder .dbt in the root directory and create a profiles.yml file"
    line8 = "5. Update profiles.yml with your database credentials - Please refer Readme for more details [https://github.com/jmangroup/snowball_dbt#]"
    line9 = "Press Enter to continue "

    # Get terminal width
    term_width = shutil.get_terminal_size().columns

    # Center the output lines relative to terminal width
    print(line1)
    print(line2)
    print(line3)
    rotating_slash_after(line4, 5)
    rotating_slash_after(line5, 1)
    print(line6)
    print(line7)
    print(line8)
    blinking_dots_input(line9)
    print("\n")
    print(line1)
    print("\n")

def show_progress(desc, duration=None, steps=None):
    """Show a progress bar for a given operation"""
    if duration:
        # Time-based progress bar
        with tqdm(total=100, desc=desc, bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {elapsed}') as pbar:
            step_time = duration / 100
            for i in range(100):
                time.sleep(step_time)
                pbar.update(1)
    elif steps:
        # Step-based progress bar
        pbar = tqdm(total=steps, desc=desc, bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt}')
        return pbar
    else:
        # Indeterminate progress bar
        return tqdm(desc=desc, bar_format='{desc}: Processing...')

def cleanup_previous_run():
    """Clean up previous compiled files and notebooks"""
    for dir_path in [compiled_dir, notebooks_dir]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

def get_dbt_models_count():
    """Count the number of dbt models in the project"""
    models_dir = os.path.join(project_dir, "models")
    model_count = 0
    if os.path.exists(models_dir):
        for root, _, files in os.walk(models_dir):
            model_count += len([f for f in files if f.endswith('.sql')])
    return max(model_count, 1)  # At least 1 to avoid division by zero

def run_dbt_deps(dbname, schemaname, tablename):
    """Run dbt deps to install dependencies"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    deps_args = [
        "deps",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]
    
    # Show progress bar with estimated steps
    with tqdm(total=100, desc="Installing dependencies", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
        dbt = dbtRunner()
        # Capture stdout/stderr to suppress logs
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        # Simulate progress during dependency installation
        pbar.update(20)
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(deps_args)
        pbar.update(80)
        
        pbar.set_description("Dependencies installed" if result.success else "❌ Dependencies failed")
    
    return result

def run_dbt_seed(dbname, schemaname, tablename):
    """Run dbt seed to update the user mapping file"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    deps_args = [
        "seed",
        "--select", "column_mapping",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]
    
    # Show progress bar with estimated steps
    with tqdm(total=100, desc="Updating mapping file", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
        dbt = dbtRunner()
        # Capture stdout/stderr to suppress logs
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        # Simulate progress during dependency installation
        pbar.update(20)
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(deps_args)
        pbar.update(80)
        
        pbar.set_description("Updated mapping file" if result.success else "❌ Failed to map seed file")
    
    return result

def connection_check(dbname, schemaname, tablename):
    """Run dbt debug to check connection"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    debug_args = [
        "debug",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]
    
    # Show progress bar with estimated steps
    with Progress(
    TextColumn("{task.description}"),
    TextColumn(": {task.percentage:>3.0f}%"),
    TextColumn("|"),
    BarColumn(bar_width=bar_width, style="green", complete_style="green"),
    TextColumn("|"),
    ) as pbar:
        task = pbar.add_task("Establishing Connection", total=100)

        dbt = dbtRunner()
        # Capture stdout/stderr to suppress logs
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        # Simulate progress during dependency installation
        pbar.update(task,20)
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(debug_args)
        pbar.update(task,80)
        
        pbar.set_description("Establishing Connection" if result.success else "❌ Failed")
    
    return result

def run_dbt(dbname, schemaname, tablename):
    """Run all dbt models with detailed progress tracking"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    run_args = [
        "run",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]
    
    # Get estimated model count for progress tracking
    model_count = get_dbt_models_count()
    
    with tqdm(total=model_count, desc="Running dbt models", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt} models') as pbar:
        dbt = dbtRunner()
        
        # Create a custom handler to track progress
        class ProgressTracker:
            def __init__(self, pbar):
                self.pbar = pbar
                self.processed = 0
                
            def update_progress(self):
                if self.processed < model_count:
                    self.processed += 1
                    self.pbar.update(1)
        
        tracker = ProgressTracker(pbar)
        
        # Simulate progress updates during model execution
        def simulate_model_progress():
            import threading
            import time
            for i in range(model_count):
                time.sleep(0.5)  # Simulate processing time
                if tracker.processed < model_count:
                    tracker.update_progress()
        
        # Start progress simulation in background
        progress_thread = threading.Thread(target=simulate_model_progress)
        progress_thread.daemon = True
        progress_thread.start()
        
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(run_args)
        
        # Ensure progress bar reaches 100%
        remaining = model_count - tracker.processed
        if remaining > 0:
            pbar.update(remaining)
        
        pbar.set_description("✅ dbt models completed" if result.success else "❌ dbt models failed")
    
    return result

def run_pre_run_setup(dbname, schemaname, tablename):
    """Run the pre_run_setup macro with progress tracking"""
    args_dict = {
        'db_name': dbname,
        'schema_name': schemaname,
        'table_name': tablename
    }
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    args_str = json.dumps(args_dict)
    macro_args = [
        "run-operation",
        "pre_run_setup",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--args", args_str,
        "--vars", vars_str
    ]
    
    # Pre-run setup typically involves multiple steps
    with tqdm(total=100, desc="Running Pre setup Macro", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
        dbt = dbtRunner()
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        # Simulate progress steps
        pbar.update(25)  # Initializing
        time.sleep(0.2)
        pbar.update(25)  # Validating connections
        
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(macro_args)
        
        pbar.update(50)  # Macro execution completed
        pbar.set_description("\nPre-setup completed" if result.success else "\nPre-setup failed")
    
    return result

def build_dbt_compile_args(dbname, schemaname, tablename):
    """Build arguments for dbt compile"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    return [
        "compile",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]

def run_dbt_args(cli_args, dbname, schemaname, tablename):
    """Run dbt with given arguments, with detailed compilation progress."""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    cli_args += ["--vars", vars_str]

    # Check if this is a compile operation for enhanced progress tracking
    is_compile = "compile" in cli_args
    
    if is_compile:
        model_count = get_dbt_models_count()
        with tqdm(total=model_count, desc="Compiling dbt models", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt} models') as pbar:
            dbt = dbtRunner()
            
            # Simulate compilation progress
            def simulate_compile_progress():
                import threading
                import time
                for i in range(model_count):
                    time.sleep(0.3)  # Compilation is typically faster than runs
                    if i < model_count:
                        pbar.update(1)
            
            # Start progress simulation
            progress_thread = threading.Thread(target=simulate_compile_progress)
            progress_thread.daemon = True
            progress_thread.start()
            
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(cli_args)
            
            # Ensure we reach 100%
            pbar.n = model_count
            pbar.refresh()
            pbar.set_description("✅ dbt compilation completed" if result.success else "❌ dbt compilation failed")
    else:
        # For non-compile operations, use simple progress
        with tqdm(total=100, desc="Running dbt command", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            pbar.update(30)
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(cli_args)
            pbar.update(70)
            
            pbar.set_description("✅ dbt command completed" if result.success else "❌ dbt command failed")
    
    return result

def zip_directory(source_dir, zip_path):
    """Zip the contents of an entire directory"""
    # Count total files first
    total_files = 0
    for root, _, files in os.walk(source_dir):
        total_files += len(files)
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        with tqdm(total=total_files, desc="Creating archive", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arcname)
                    pbar.update(1)

def run_sqlfluff_on_directory(directory_path, project_root):
    """
    Run SQLFluff fix on all SQL files in a directory.
    """
    try:
        # Collect all SQL files
        sql_files = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
        
        if not sql_files:
            return True
        
        success_count = 0
        with tqdm(total=len(sql_files), desc="Applying SQLFluff", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
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
                        success_count += 1
                        
                except Exception:
                    pass
                
                pbar.update(1)
        
        return success_count > 0
            
    except Exception:
        return False

def apply_sqlfluff_to_compiled(project_root):
    """
    Apply SQLFluff to all compiled SQL files before packaging.
    Run SQLFluff on the entire compiled models directory at once for efficiency.
    """
    # check SQLFluff is availability
    try:
        subprocess.run(["sqlfluff", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        with tqdm(desc="❌ SQLFluff not available", bar_format='{desc}') as pbar:
            time.sleep(1)
        return False
    
    # Run SQLFluff on the compiled models directory
    return run_sqlfluff_on_directory(compiled_dir, project_root)

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

        with tqdm(total=len(model_folders), desc="📓 Generating notebooks", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
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
                    
                pbar.update(1)
                
    except Exception:
        return False
    return True

def copy_seed_file(seed_path, target_dir,dbname, schemaname, tablename):
    """
    Copy the seed file to target directory and ensure it's named column_mapping.csv
    """
    os.makedirs(target_dir, exist_ok=True)
    target_file = os.path.join(target_dir, "column_mapping.csv")

    try:
        # Check if source and target are the same file
        if os.path.abspath(seed_path) == os.path.abspath(target_file):
            pass
        else:
            # Copy the file (will overwrite if exists)
            shutil.copy(seed_path, target_file)
        
        run_dbt_seed(dbname, schemaname, tablename)
        return True

    except FileNotFoundError:
        with tqdm(desc="❌ Mapping file not found", bar_format='{desc}') as pbar:
            time.sleep(1)
        return False
    except Exception:
        with tqdm(desc="❌ Failed to process mapping file", bar_format='{desc}') as pbar:
            time.sleep(1)
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
                return
        else:
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

    except Exception:
        pass

def process_compiled_sql_files():
    """Walk through compiled models directory and transform all SQL files."""
    # Count SQL files first
    sql_files = []
    for root, _, files in os.walk(compiled_dir):
        for file in files:
            if file.endswith(".sql"):
                sql_files.append(os.path.join(root, file))
    
    with tqdm(total=len(sql_files), desc="Transforming SQL files", bar_format='{desc}: {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
        for sql_file in sql_files:
            transform_compiled_sql(sql_file)
            pbar.update(1)

def remove_readonly_files(func, path, _):
    """Error handler for removing read-only files on Windows"""
    import stat
    os.chmod(path, stat.S_IWRITE)
    func(path)

def clone_repo(git_url: str) -> str:
    """
    Clone the Git repository from `git_url` into the current directory.
    
    Args:
        git_url (str): URL of the git repository to clone.
        
    """
    repo_name = os.path.splitext(os.path.basename(git_url))[0]
    clone_location = os.path.join(Path.home(), "Downloads", repo_name)
    clone_path = os.path.join(Path.home(), "Downloads", repo_name)

    
    # If folder exists, remove it properly
    if os.path.exists(clone_location):
        try:
            # Use the error handler for Windows read-only files
            shutil.rmtree(clone_location, onerror=remove_readonly_files)
        except Exception as e:
            # If still fails, try alternative approach
            try:
                import subprocess
                if os.name == 'nt':  # Windows
                    subprocess.run(['rmdir', '/s', '/q', clone_location], shell=True, check=True)
                else:  # Unix/Linux
                    subprocess.run(['rm', '-rf', clone_location], check=True)
            except:
                print("❌ Failed to remove existing repository")
                raise Exception(f"Could not remove existing directory: {clone_location}")
    
    try:
        Repo.clone_from(git_url, clone_location)
    except Exception as e:
        print("❌ Failed to clone repository")
        raise Exception(f"Failed to clone repository: {str(e)}")
    
    # Change to the cloned directory
    # os.chdir(clone_path)
    return os.path.join(clone_location, 'seeds', 'column_mapping.csv')


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
    
    try:
        os.chmod(dest_path, 0o666)  # read/write for owner, read for group/others
    except Exception as e:
        print(f"Warning: Could not set permissions on {dest_path}: {e}")
    
    return str(dest_path)

def main():
    welcome_message()

    # Clean up previous runs
    cleanup_previous_run()

    # Clone the latest repo from Snowball dbt
    mapping_file_path = clone_repo("https://github.com/jmangroup/snowball_dbt.git")
    copy_csv_to_downloads(mapping_file_path)

    initial_set_up()

    project_root = project_dir
    text = f"Checking Database Connection!"
    width = len(text) + 8  # padding for stars
    border = "*" * width

    # Prepare the lines to print
    line1 = border
    line2 = " " + text.center(width - 2)
    line3 = ("*" * (len(line2) // 3)).center(width - 2)
    print(line1)
    print(line2)
    print(line3)
    tablename = input('Enter revenue table name : ')
    db_config = load_dbt_profile("Snowball_dbt", "dev")
    dbname = db_config.get("database")
    schemaname = db_config.get("schema")
    type = db_config.get("type")

    print(f"Revenue table: {dbname}.{schemaname}.{tablename}")
    
    print("Available Database Platform")
    print("     1: Snowflake")
    print("     2: Databricks")
    print("     3: Fabric")
    print("     4: SQL databse")
    print("     5: Redshift --In Progress")

    platform_dict = {
        1: "snowflake",
        2: "databricks",
        3: "fabric",
        4: "sqlserver",
        5: "redshift"
    }
    user_choice = int(input("\nSelect your Database Platform [1-5]: ").strip())
    while():
        try:
            if user_choice in [1, 2, 3, 4, 5]:
                break
            else: user_choice = int(input("❌ Invalid input. Please enter [1-5]: ").strip())
        except ValueError:
            print("❌ Invalid input. Please enter [1-5].")

    def checking():
        connection = connection_check(dbname,schemaname,tablename)
        if not connection.success or type != platform_dict.get(user_choice):
            print("\U0001F641 Connection Failed! \n")
            blinking_dots_input("Update Your Profiles.yml correctly and Press Enter to check the connection again!! ")
            checking()
        if connection.success:
            # rotating_slash_after(line4,8,1)
            print("\U0001F642 Connection Established Successfully! \n")
    checking()

    text = f"Database Platform | Snowball Version!"
    width = len(text) + 8  # padding for stars
    border = "*" * width

    # Prepare the lines to print
    line1 = border
    line2 = " " + text.center(width - 2)
    line3 = ("*" * (len(line2) // 3)).center(width - 2)
    print(line1)
    print(line2)
    print(line3)

    deps_result = run_dbt_deps(dbname, schemaname, tablename)
    if not deps_result.success:
        print("❌ dbt deps failed")
        return

    if not os.path.exists(mapping_file):
        print(f"❌ Mapping file not found at the specified path: {mapping_file}")
        sys.exit(1)

    copy_seed_file(mapping_file, dbt_seed_dir, dbname, schemaname, tablename)

    print(line3)
    print("Available Snowball Version")
    print("     1: dbt")
    print("     2: sql")
    print("     3: Spark sql")
    print("     4: Redshift - N/A")

    final_text = "  Thanks for using Snowball Product! Happy coding! \U0001F642  "
    # Get terminal width
    term_width = shutil.get_terminal_size().columns
    try:
        user_choice_version = int(input("\nSelect your Snowball Version: ").strip())
    except ValueError:
        print("❌ Invalid input. Please enter [1-4].")
        return
    
    print(f"{line1}\n")
    if user_choice_version == 1:
        if user_choice == 1:
            text = "Generating Snowflake adaptable dbt code "
        if user_choice == 2:
            text = "Generating Databricks adaptable dbt code ..."
        if user_choice == 3:
            text = "Generating Fanric adaptable dbt code ..."

        try:
            rotating_slash_after(text, 10)
            output_zip    = os.path.join(output_dir, "snowball_dbt.zip")
            zip_directory(project_dir, output_zip)
            print(f"snowball_dbt code is saved at: {output_zip}\n")
            print(final_text.center(term_width, '*'))
        except Exception as e:
            print(f"❌ Failed to zip dbt project: {e}")

    elif user_choice_version == 2:
        print("\nGenerating SQL code...\n")
        
        try:
            output_zip    = os.path.join(output_dir, "snowball_sql.zip")
            macro_result = run_pre_run_setup(dbname, schemaname, tablename)
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            run_result = run_dbt(dbname, schemaname, tablename)
            if not run_result.success:
                print("❌ dbt run failed")
                return
                
        except Exception as e:
            print(f"❌ dbt run failed: {e}")
            return
        
        compile_args = build_dbt_compile_args(dbname, schemaname, tablename)
        compile_result = run_dbt_args(compile_args, dbname, schemaname, tablename)

        if compile_result.success:
            sqlfluff_success = apply_sqlfluff_to_compiled(project_root)
            if not sqlfluff_success:
                with tqdm(desc="SQLFluff issues detected", bar_format='{desc}') as pbar:
                    time.sleep(1)
                    
            process_compiled_sql_files()
            zip_directory(compiled_dir, output_zip)
            print(f"snowball_sql code is generated successfully and saved at: {output_zip}\n")
            print(final_text.center(term_width, '*'))
        else:
            print("❌ dbt compile failed")

    elif user_choice_version == 3:
        print("\nGenerating Spark SQL notebooks...\n")
            
        try:
            output_zip = os.path.join(output_dir, "snowball_spark.zip")
            macro_result = run_pre_run_setup(dbname, schemaname, tablename)
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            run_result = run_dbt(dbname, schemaname, tablename)
            if not run_result.success:
                print("❌ dbt run failed")
                return
                
        except Exception as e:
            print(f"❌ dbt run failed: {e}")
            return

        compile_args = build_dbt_compile_args(dbname, schemaname, tablename)
        result = run_dbt_args(compile_args, dbname, schemaname, tablename)

        if result and result.success:
            sqlfluff_success = apply_sqlfluff_to_compiled(project_root)
            if not sqlfluff_success:
                with tqdm(desc="⚠️ SQLFluff issues detected", bar_format='{desc}') as pbar:
                    time.sleep(1)

            generate_notebooks()

            if os.path.exists(output_zip):
                os.remove(output_zip)
            zip_directory(notebooks_dir, output_zip)
            print(f"Snowball Spark SQL Notebooks are zipped and saved at: {output_zip}\n")
            print(final_text.center(term_width, '*'))

        else:
            print("❌ dbt compile failed")
    elif user_choice_version == 4:
        print("\nRedshift Version is in Progress! Please contact Snowball product team.")
    else:
        print("❌ Invalid choice. Please enter [1-4]")

if __name__ == "__main__":
    main()