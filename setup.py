"""Setup function for otf-addons-aws package."""

from setuptools import find_namespace_packages, setup

setup(
    name="otf-addons-aws",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
)
