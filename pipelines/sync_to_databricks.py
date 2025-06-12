import os
import requests
import base64

# Load environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    raise EnvironmentError("Databricks credentials not found in environment variables.")

# Dynamically determine project root and file locations
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))                  # /pipeline
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))          # /
SELECTIVE_FILES_PATH = os.path.join(PROJECT_ROOT, "pipelines", "selective_files.txt")
DATABRICKS_WORKSPACE_BASE = "/Workspace/Shared"

def get_file_list(file_path):
    with open(file_path, "r") as f:
        return [line.strip().lstrip("/\\") for line in f if line.strip()]

def create_workspace_folder(workspace_dir):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/mkdirs"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {"path": workspace_dir}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"Warning: Folder may already exist or error: {response.text}")

def upload_file_to_workspace(local_path, workspace_path):
    if not os.path.exists(local_path):
        print(f"❌ Local file not found: {local_path}")
        return
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {
        "path": workspace_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": content,
        "overwrite": True
    }
    response = requests.post(url, headers=headers, json=data)
    status = "✅" if response.status_code == 200 else "❌"
    print(f"{status} Uploaded to {workspace_path}: {response.status_code} - {response.text}")

def main():
    files = get_file_list(SELECTIVE_FILES_PATH)
    for rel_path in files:
        rel_path = rel_path.replace("\\", "/").lstrip("/")
        local_file = os.path.join(PROJECT_ROOT, rel_path)
        workspace_path = os.path.join(DATABRICKS_WORKSPACE_BASE, rel_path).replace("\\", "/")
        create_workspace_folder(os.path.dirname(workspace_path))
        upload_file_to_workspace(local_file, workspace_path)

if __name__ == "__main__":
    main()