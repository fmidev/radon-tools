#
# SConscript for neons-tools
#

Import('env')
import os

env.Program(target = 'grid_to_neons', source = ['main/grid_to_neons.cpp', 'source/BDAPLoader.cpp', 'source/NetCDFLoader.cpp', 'source/GribLoader.cpp'])
env.Program(target = 'create_grid_tables', source = ['main/create_grid_tables.cpp', 'source/GribCreate.cpp'])
