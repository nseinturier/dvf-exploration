import src
from pathlib import Path
from pydantic import BaseModel
import os

# Constants
PACKAGE_ROOT = Path(src.__file__).resolve().parent
ROOT = PACKAGE_ROOT.parent

class Config(BaseModel):
    """General configuration for the project."""
    root: Path = ROOT
    data_dir: Path = ROOT / "data"
    jinka_email: str = os.environ.get("EMAIL")
    jinka_password: str = os.environ.get("PASSWORD")
    
config = Config()