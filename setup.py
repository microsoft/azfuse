from setuptools import setup


with open('requirements.txt') as fp:
    reqs = [l.strip() for l in fp]

description = 'AzFuse: a lightweight blobfuse-like python tool'
setup(name='azfuse',
      version='0.1',
      description=description,
      packages=['azfuse'],
      install_requires=reqs,
     )

