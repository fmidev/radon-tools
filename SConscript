#
# SConscript for neons-tools
#

Import('env')
import os

env.Program(target = 'grid_to_radon', 
            source = [
                'main/grid_to_radon.cpp',
                'source/netcdfloader.cpp',
                'source/geotiffloader.cpp',
                'source/gribloader.cpp',
                'source/s3gribloader.cpp',
                'source/common.cpp'
            ])
