 
from setuptools import setup, find_packages

with open('LICENSE') as license_file:
    license_str= license_file.read()
    
setup(name='data_distiller',
      version='0.9.0',
      description='Generic module used to process/extract data.',
      author='Claudio Romero',
      license=license_str,
      package_dir={"": "src"},
      packages=find_packages(where="src"),
      python_requires=">=3.6",
      install_requires=["liboptions >= 0.9.0"])
