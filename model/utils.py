import json

def load_json(filename):
    """Load a json file"""
    try:
        with open(filename) as f:
            data = json.load(f)
    except (FileNotFoundError, TypeError, json.decoder.JSONDecodeError) as ex:
        print('Failed to load {:s} | {}'.format(filename, ex))
        data = None

    return data
