#
# SConscript for neons-tools

import os

AddOption(
    '--debug-build',
    dest='debug-build',
    action='store_true',
    help='debug build',
    default=False)

env = Environment()

# Check build

DEBUG = GetOption('debug-build')
RELEASE = (not DEBUG)

# Assign compilers

if os.environ.get('CC') != None:
        env['CC'] = os.environ.get('CC')
else:
	env['CC'] = 'gcc'

if os.environ.get('CXX') != None:
        env['CXX'] = os.environ.get('CXX')
else:
	env['CXX'] = 'g++'

# cuda toolkit path

cuda_toolkit_path = '/usr/local/cuda-6.5'

if os.environ.get('CUDA_TOOLKIT_PATH') is None:
        print "Environment variable CUDA_TOOLKIT_PATH not set, assuming " + cuda_toolkit_path
else:
        cuda_toolkit_path = os.environ['CUDA_TOOLKIT_PATH']

have_cuda = False

if os.path.isfile(cuda_toolkit_path + '/lib64/libcudart.so'):
        have_cuda = True

# Includes

includes = []

if os.environ.get('INCLUDE') != None:
        includes.append(os.environ.get('INCLUDE'))

includes.append('include')

if os.environ.get('ORACLE_HOME') is None:
        includes.append('/usr/include/oracle')
else:
        includes.append(os.environ.get('ORACLE_HOME') + '/rdbms/public')

env.Append(CPPPATH = includes)

# Library paths

librarypaths = []

librarypaths.append('/usr/lib64')
librarypaths.append('/usr/lib64/oracle')
librarypaths.append('/home/partio/workspace/fmidb/lib')
librarypaths.append('/home/partio/workspace/fminc/lib')
librarypaths.append('/home/partio/workspace/fmigrib/lib')

env.Append(LIBPATH = librarypaths)

# Libraries

libraries = []

libraries.append('fmidb')
libraries.append('fminc')
libraries.append('odbc')
libraries.append('clntsh')
libraries.append('grib_api')
libraries.append('netcdf_c++')

env.Append(LIBS = libraries)

env.Append(LIBS=env.File('/usr/lib64/libfmigrib.a'))
env.Append(LIBS=['grib_api'])

boost_libraries = [ 'boost_program_options', 'boost_filesystem', 'boost_system', 'boost_regex', 'boost_iostreams', 'boost_thread' ]

env.Append(LIBS = boost_libraries)

if have_cuda:
	env.Append(LIBS=env.File(cuda_toolkit_path + '/lib64/libcudart_static.a'))

# CFLAGS

# "Normal" flags

cflags_normal = []
cflags_normal.append('-Wall')
cflags_normal.append('-W')
cflags_normal.append('-Wno-unused-parameter')
cflags_normal.append('-Werror')

# Extra flags

cflags_extra = []
cflags_extra.append('-Wpointer-arith')
cflags_extra.append('-Wcast-qual')
cflags_extra.append('-Wcast-align')
cflags_extra.append('-Wwrite-strings')
cflags_extra.append('-Wconversion')
cflags_extra.append('-Winline')
cflags_extra.append('-Wnon-virtual-dtor')
cflags_extra.append('-Wno-pmf-conversions')
cflags_extra.append('-Wsign-promo')
cflags_extra.append('-Wchar-subscripts')
cflags_extra.append('-Wold-style-cast')

# Difficult flags

cflags_difficult = []
cflags_difficult.append('-pedantic')
# cflags_difficult.append('-Weffc++')
cflags_difficult.append('-Wredundant-decls')
cflags_difficult.append('-Wshadow')
cflags_difficult.append('-Woverloaded-virtual')
#cflags_difficult.append('-Wunreachable-code') will cause errors from boost headers
cflags_difficult.append('-Wctor-dtor-privacy')

# Default flags (common for release/debug)

cflags = []
cflags.append('-std=c++0x')

env.Append(CCFLAGS = cflags)
env.Append(CCFLAGS = cflags_normal)

# Linker flags

env.Append(LINKFLAGS = ['-Wl,--warn-unresolved-symbols','-Wl,--as-needed'])

# '-Wl,-rpath,.'

# Defines

env.Append(CPPDEFINES=['UNIX'])

build_dir = ""

if RELEASE:
	env.Append(CCFLAGS = ['-O2'])
	env.Append(CPPDEFINES = ['NDEBUG'])
	build_dir = "build/release"

if DEBUG:
	env.Append(CPPDEFINES = ['DEBUG'])
	env.Append(CCFLAGS = ['-O0', '-g'])
	env.Append(CCFLAGS = cflags_extra)
	env.Append(CCFLAGS = cflags_difficult)
	build_dir = "build/debug"

SConscript('SConscript', exports = ['env'], variant_dir=build_dir, duplicate=0)
Clean('.', build_dir)
