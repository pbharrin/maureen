try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='Maureen',
      version='0.1',
      description='Python Distributed Data Mining Library',
      author='Peter Harrington',
      author_email='peter.b.harrington@gmail.com',
      url='https://github.com/pbharrin/maureen',
      packages=['maureen', 
      		'maureen.adapters',
      		'maureen.classify',
      		'maureen.cluster',
      		'maureen.recommend',
		],
      install_requires=[
        'mrjob'
    		],
     
     )

