import yaml
from pathlib import Path


def parse_pipeline(file_path: str):
    """
    Parse and validate pipeline YAML file
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Pipeline file not found: {file_path}")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    if "name" not in data:
        raise ValueError("Pipeline must have a 'name' field")

    if "steps" not in data:
        raise ValueError("Pipeline must contain 'steps'")

    for step in data["steps"]:
        if "name" not in step:
            raise ValueError("Each step must have a 'name'")
        if "command" not in step:
            raise ValueError("Each step must have a 'command'")
        if "retry" not in step:
            step["retry"] = 0

    return data