from importlib.metadata import version
from importlib.resources import files

from soul_plugin_core import __version__


def test_package_import():
    assert __version__ == "0.1.0"


def test_package_includes_type_marker():
    assert files("soul_plugin_core").joinpath("py.typed").is_file()


def test_package_version_matches_installed_distribution():
    assert version("soul-plugin-core") == __version__
