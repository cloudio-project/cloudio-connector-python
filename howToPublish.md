# How to publish to pypi

1. Change the version number into the pyproject.toml file
2. python -m pip install build twine
3. python -m build
4. twine upload dist/*

For more information, visit https://realpython.com/pypi-publish-python-package