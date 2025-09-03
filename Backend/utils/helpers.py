import types

def make_json_safe(obj):
    """Recursively convert generators to lists for JSON serialization."""
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(item) for item in obj]
    elif isinstance(obj, types.GeneratorType):
        return list(obj)
    return obj
