"""Setup file
Tutorial:
  http://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/quickstart.html
rm -rf dist/
python setup.py sdist bdist_wheel
cd ..
pip install -I Pipelineorchestration/dist/Pipelineorchestration-1.0-py3-none-any.whl  # Must be outside the project root
cd Pipelineorchestration
"""
import setuptools  # this is for bdist wheel

from distutils.core import setup

setuptools.setup(
    name="Dataprocessing",
    version=1.,
    author_email="yogitasn@yahoo.com",
    url="",
    packages=setuptools.find_packages(),
    package_dir={
        "Dataprocessing": "Dataprocessing",
        "Dataprocessing.blockface_processing": "Dataprocessing/blockface_processing",
        "Dataprocessing.occupancy_processing": "Dataprocessing/occupancy_processing",
        "Dataprocessing.job_tracker": "Dataprocessing/job_tracker",
        "Dataprocessing.utilities": "Dataprocessing/utilities"   
        
    },
    python_requires=">=3.7",
    data_files=[('config', ['Pipelineorchestration/config.cfg'])],
    license=""
)
