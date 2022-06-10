from setuptools import setup
from pip.req import parse_requirements


install_reqs = parse_requirements('requirements.txt')
reqs = [str(ir.req) for ir in install_reqs]

description = 'AzFuse: a lightweight blobfuse-like python tool'
setup(name='azfuse',
      version='0.1',
      description=description,
      packages=['azfuse'],
      install_requires=reqs,
     )

