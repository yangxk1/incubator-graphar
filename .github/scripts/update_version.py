import sys
import re
import os

def update_version():
    file_path = "pyproject.toml"
    
    suffix = os.getenv("COMMIT_SHORT_SHA", "0") 
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    match = re.search(r'version\s*=\s*"(.*?)"', content)
    
    if not match:
        print("Error: version not found in pyproject.toml")
        sys.exit(1)

    old_version = match.group(1)

    new_version = f"{old_version}{suffix}"
    print(f"Build GraphAr Version: {new_version}")

    new_content = content.replace(f'version = "{old_version}"', f'version = "{new_version}"')
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(new_content)
        
    print("pyproject.toml updated successfully.")

if __name__ == "__main__":
    update_version()
