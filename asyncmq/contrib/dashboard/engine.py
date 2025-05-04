from pathlib import Path

from lilya.templating import Jinja2Template

BASE_DIR = Path(__file__).parent

templates = Jinja2Template(directory=str(BASE_DIR / "templates"))
