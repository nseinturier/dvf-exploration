from src.core import config
from src.utils import save_json
import json
from shapely.geometry.polygon import Polygon
from tqdm import tqdm

def load_cadastre_data()->dict[str, list]:
    with open(config.data_dir / "cadastre" / "code-coords.json", "r") as f:
        polygon_data = json.load(f)
        polygon_data = {k: Polygon(v) for k, v in polygon_data.items()}
    return polygon_data


def get_adjency_cadastre(
        polygon_data: dict[str, Polygon]
)->dict[str, list[str]]:
    all_adjacing_cadastres = dict()

    for cadastre, polygon in tqdm(polygon_data.items()):
        adjacing_cadastres = []

        for other_cadastre, other_polygon in polygon_data.items():
            
            if polygon.touches(other_polygon):
                adjacing_cadastres.append(other_cadastre)

        all_adjacing_cadastres[cadastre] = adjacing_cadastres
    return all_adjacing_cadastres

if __name__ == "__main__":
    polygon_data = load_cadastre_data()
    adjency_cadastre = get_adjency_cadastre(polygon_data)
    save_json(
        file_path = config.data_dir / "cadastre" / "adjency_cadastre.json",
        json_input = adjency_cadastre
    )