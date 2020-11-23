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

# Includes

includes = []

if os.environ.get('INCLUDE') != None:
        includes.append(os.environ.get('INCLUDE').split(":"))

includes.append('include')
includes.append('/usr/include/himan')
includes.append('/usr/include/himan/plugins')
includes.append('/usr/include/gdal')

env.Append(CPPPATH = includes)

# Library paths

librarypaths = []

if os.environ.get('LIBRARYPATH') != None:
        librarypaths.append(os.environ.get('LIBRARYPATH').split(":"))

librarypaths.append('/usr/lib64')
librarypaths.append('/usr/lib64/boost169')

env.Append(LIBPATH = librarypaths)

# Libraries

libraries = []

libraries.append('himan')
libraries.append('fmidb')
libraries.append('fminc')
libraries.append('pqxx')
libraries.append('odbc')
libraries.append('netcdf_c++')
libraries.append('s3')
libraries.append('fmigrib')
libraries.append('eccodes')
libraries.append('gdal')
libraries.append('fmt')

env.Append(LIBS = libraries)

boost_libraries = [ 'boost_program_options', 'boost_filesystem', 'boost_system', 'boost_regex', 'boost_iostreams', 'boost_thread' ]

env.Append(LIBS = boost_libraries)

#if env['HAVE_CUDA']:
#	env.Append(LIBS=env.File('/usr/local/cuda/lib64/libcudart_static.a'))

env.Append(LIBS = ['fmt','dl','rt'])

# CFLAGS

# "Normal" flags

cflags_normal = []
cflags_normal.append('-Wall')
cflags_normal.append('-W')
cflags_normal.append('-Wno-unused-parameter')
#cflags_normal.append('-Werror')

# Extra flags

cflags_extra = []
cflags_extra.append('-Wpointer-arith')
cflags_extra.append('-Wcast-qual')
cflags_extra.append('-Wcast-align')
cflags_extra.append('-Wwrite-strings')
cflags_extra.append('-Wconversion')
cflags_extra.append('-Wnon-virtual-dtor')
cflags_extra.append('-Wno-pmf-conversions')
cflags_extra.append('-Wsign-promo')
cflags_extra.append('-Wchar-subscripts')
cflags_extra.append('-Wold-style-cast')

# Difficult flags

cflags_difficult = []
cflags_difficult.append('-pedantic')
#cflags_difficult.append('-Weffc++')
cflags_difficult.append('-Wredundant-decls')
cflags_difficult.append('-Wshadow')
cflags_difficult.append('-Woverloaded-virtual')
cflags_difficult.append('-Wunreachable-code')
cflags_difficult.append('-Wctor-dtor-privacy')

# Default flags (common for release/debug)

cflags = []
cflags.append('-std=c++11')

env.Append(CCFLAGS = '-fPIC')
env.Append(CCFLAGS = cflags)
env.Append(CCFLAGS = cflags_normal)
env.Append(CCFLAGS = cflags_extra)
env.Append(CCFLAGS = cflags_difficult)

env.AppendUnique(CCFLAGS=('-isystem', '/usr/include/boost169'))

# Linker flags

env.Append(LINKFLAGS = ['-Wl,--export-dynamic', '-rdynamic', '-Wl,--as-needed', '-Wl,--warn-unresolved-symbols', '-pthread'])

# '-Wl,-rpath,.'

# Defines

env.Append(CPPDEFINES=['UNIX'])

build_dir = ""


if RELEASE:
	env.Append(CCFLAGS = ['-O2', '-g'])
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
