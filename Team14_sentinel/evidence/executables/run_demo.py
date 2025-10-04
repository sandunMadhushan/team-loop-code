import subprocess
import sys
import os

def run_command(command):
    """Runs a command and prints its output."""
    try:
        print(f"--- Running command: {' '.join(command)} ---")
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line, end='')
        process.wait()
        if process.returncode != 0:
            print(f"--- Command failed with exit code {process.returncode} ---")
            sys.exit(process.returncode)
        print("--- Command completed successfully ---\n")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

def main():
    """
    Main execution function to set up, run the pipeline, and launch the dashboard.
    This script is run from the `evidence/executables` directory.
    """
    # Get the correct python executable
    python_executable = sys.executable

    # Define relative paths
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    requirements_path = os.path.join(project_root, 'requirements.txt')
    pipeline_script_path = os.path.join(project_root, 'src', 'main.py')
    dashboard_script_path = os.path.join(project_root, 'src', 'dashboard.py')

    # 1. Install dependencies
    print("Step 1: Installing dependencies from requirements.txt...")
    run_command([python_executable, '-m', 'pip', 'install', '-r', requirements_path])

    # 2. Run the data processing pipeline
    print("Step 2: Running the data processing pipeline to generate events.jsonl...")
    run_command([python_executable, pipeline_script_path])

    # 3. Launch the Streamlit dashboard
    print("Step 3: Launching the Streamlit dashboard...")
    print("You can view the dashboard in your browser at the URL provided by Streamlit.")
    run_command([python_executable, '-m', 'streamlit', 'run', dashboard_script_path])

if __name__ == "__main__":
    main()