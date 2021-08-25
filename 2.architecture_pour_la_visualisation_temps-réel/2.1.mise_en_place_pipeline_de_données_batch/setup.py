
from setuptools import setup, find_packages

setup(
    name="dataflow_pipeline_dependencies",
    version="0.1",
    install_requires=[
   'pandas==0.25.2',
    ],
    packages = find_packages()
)
