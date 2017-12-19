from setuptools import setup
from Cython.Build import cythonize
import numpy

setup(name='histwords',
      version='0.1',
      description='Code for manipulating historical word vector embeddings.',
      url='https://github.com/williamleif/histwords',
      author='William Hamilton',
      author_email='wleif@stanford.edu',
      license='Apache Version 2',
      ext_modules = cythonize(["representations/sparse_io.pyx", "representations/sparse_io_ref.pyx", "googlengram/pullscripts/merge.pyx"]),
      include_dirs=[numpy.get_include()],
      install_requires = ['numpy',
                          'cython',
                          'sklearn',
                          'statsmodels']
      )
