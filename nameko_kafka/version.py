"""
Version for nameko_kafka package
"""

__version__ = '0.1.0'  # pragma: no cover


def version_info():  # pragma: no cover
    """
    Get version of nameko_kafka package as tuple
    """
    return tuple(map(int, __version__.split('.')))  # pragma: no cover
