__version__ = "0.1"

def is_release_version():
    return not any(__version__.isalpha() for l in __version__)