from distutils.core import setup, Extension
import numpy.distutils.misc_util 

ext = Extension("fj", sources=["fj_module.c"])
setup(name="fj",ext_modules=[ext], include_dirs=numpy.distutils.misc_util.get_numpy_include_dirs())
