from src.core import config
from tqdm import tqdm
import json

def load_lines()->list[str]:
    file_path = config.data_dir / "cadastre" / "cadastre-france-sections.json"
    lines = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in tqdm(f):
            lines.append(line)
    
    return lines

def convert_lines_to_json(lines: list)-> list[dict]:
    return [json.loads(line[:-2]) for line in tqdm(lines[1:])]

def create_json_list_polygons()->dict[str, list]:
    lines = load_lines()
    lines_json = convert_lines_to_json(lines)
    lines_nice = [c for c in tqdm(lines_json) if c["properties"]["commune"] == "06088"]
    code_coords = {line["properties"]["code"]: line["geometry"]["coordinates"][0][0] for line in lines_nice}
    save_json_code_coords(code_coords)

def save_json_code_coords(json_input: dict)->None:
    file_path = config.data_dir / "cadastre" / "code-coords.json"
    with open(file_path, "w") as f:
        json.dump(json_input, f)

if __name__=="__main__":
    create_json_list_polygons()

