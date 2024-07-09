""" Setup file """
from setuptools import setup
import versioneer


setup(    
    name = '{{cookiecutter.package_name}}',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
