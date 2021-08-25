
from setuptools import setup, find_packages

setup(
    name="dataflow_pipeline_dependencies",
    version="0.1",
    install_requires=[
   'numpy==1.17.3', 'pandas==1.2.4', 'pystan==2.19.1.1', 'prophet==1.0.1', 'pytz==2019.3'
    ],
    packages = find_packages()
)
