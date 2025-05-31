from pathlib import Path
import json

def save_json(
        file_path:Path, 
        json_input: dict
)->None:
    with open(file_path, "w") as f:
        json.dump(json_input, f)