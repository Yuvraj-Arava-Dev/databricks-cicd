import os
import requests
import base64

# Configuration via Environment Variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN =os.getenv("DATABRICKS_TOKEN")
LOCAL_BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SELECTIVE_FILES_PATH = os.path.join(LOCAL_BASE_PATH, "pipelines/selective_files.txt")
DATABRICKS_WORKSPACE_BASE = "/Workspace/Shared"

def get_file_list(file_path):
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def create_workspace_folder(workspace_dir):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/mkdirs"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    data = {
        "path": workspace_dir
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        print(f"Ensured folder exists: {workspace_dir}")
    else:
        print(f"Failed to create folder {workspace_dir}: {response.text}")

def upload_file_to_workspace(local_path, workspace_path):
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    data = {
        "path": workspace_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": content,
        "overwrite": True
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        print(f"Uploaded {local_path} to {workspace_path}")
    else:
        print(f"Failed to upload {local_path}: {response.text}")

def main():
    files = get_file_list(SELECTIVE_FILES_PATH)
    for rel_path in files:
        local_file = os.path.join(LOCAL_BASE_PATH, rel_path)
        workspace_file = os.path.join(DATABRICKS_WORKSPACE_BASE, rel_path).replace("\\", "/")
        workspace_dir = os.path.dirname(workspace_file)
        create_workspace_folder(workspace_dir)
        upload_file_to_workspace(local_file, workspace_file)

if __name__ == "__main__":
    main()