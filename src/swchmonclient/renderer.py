import tempfile
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader


def render_manifest(template_path: str, **variables: Any) -> str:
    """Render a Jinja2 manifest template and write it to a temporary file.

    The caller is responsible for deleting the returned file when done.

    Args:
        template_path: Path to the Jinja2 template file.
        **variables: Template variables passed to the Jinja2 render context.

    Returns:
        Absolute path to the rendered temporary file.
    """
    template_file = Path(template_path)
    env = Environment(loader=FileSystemLoader(str(template_file.parent)))
    template = env.get_template(template_file.name)
    rendered = template.render(**variables)

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    tmp.write(rendered)
    tmp.close()
    return tmp.name
