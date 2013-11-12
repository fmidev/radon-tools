PROG = neons-tools

MAINFLAGS = -Wall -W -Wno-unused-parameter -Werror

EXTRAFLAGS = -Wpointer-arith \
	-Wcast-qual \
	-Wcast-align \
	-Wwrite-strings \
	-Wconversion \
	-Winline \
	-Wnon-virtual-dtor \
	-Wno-pmf-conversions \
	-Wsign-promo \
	-Wchar-subscripts \
	-Wold-style-cast

DIFFICULTFLAGS = -pedantic -Weffc++ -Wredundant-decls -Wshadow -Woverloaded-virtual -Wunreachable-code -Wctor-dtor-privacy

CC = /usr/bin/g++

# Default compiler flags

CFLAGS = -fPIC -std=c++0x -DUNIX -O2 -DNDEBUG $(MAINFLAGS) 
LDFLAGS = -s

# Special modes

CFLAGS_DEBUG = -fPIC -std=c++0x -DUNIX -O0 -g -DDEBUG $(MAINFLAGS) $(EXTRAFLAGS)
CFLAGS_PROFILE = -fPIC -std=c++0x -DUNIX -O2 -g -pg -DNDEBUG $(MAINFLAGS)

LDFLAGS_DEBUG =
LDFLAGS_PROFILE =

includedir=/usr/include

INCLUDES = -I include \
           -I$(includedir) \
	   -I$(includedir)/oracle \

ifneq ($(INCLUDE), "")
  INCLUDES := $(INCLUDES) \
		$(INCLUDE)
endif

LIBS =  -L$(libdir) \
	-L/usr/lib64 \
	-L/usr/lib64/oracle \
        -lfmigrib \
	-lfmidb \
	-lfminc \
        -lclntsh \
        -lodbc \
        -lboost_program_options-mt \
        -lboost_filesystem-mt \
        -lboost_system-mt \
        -lgrib_api \
	-lnetcdf_c++


# Common library compiling template

# Installation directories

processor := $(shell uname -p)

ifeq ($(origin PREFIX), undefined)
  PREFIX = /usr
else
  PREFIX = $(PREFIX)
endif

ifeq ($(processor), x86_64)
  libdir = $(PREFIX)/lib64
else
  libdir = $(PREFIX)/lib
endif

objdir = obj
libdir = lib

includedir = $(PREFIX)/include

#ifeq ($(origin BINDIR), undefined)
#  bindir = $(PREFIX)/bin
#else
#  bindir = $(BINDIR)
#endif

# rpm variables

CWP = $(shell pwd)
BIN = $(shell basename $(CWP))

# Special modes

ifneq (,$(findstring debug,$(MAKECMDGOALS)))
  CFLAGS = $(CFLAGS_DEBUG)
  LDFLAGS = $(LDFLAGS_DEBUG)
endif

ifneq (,$(findstring profile,$(MAKECMDGOALS)))
  CFLAGS = $(CFLAGS_PROFILE)
  LDFLAGS = $(LDFLAGS_PROFILE)
endif

# Compilation directories

vpath %.cpp source main
vpath %.h include
vpath %.o $(objdir)

# How to install

INSTALL_PROG = install -m 775
INSTALL_DATA = install -m 664

# The files to be compiled

HDRS = $(patsubst include/%,%,$(wildcard *.h include/*.h))

MAINSRCS     = $(patsubst main/%,%,$(wildcard main/*.cpp))
MAINPROGS    = $(MAINSRCS:%.cpp=%)
MAINOBJS     = $(MAINSRCS:%.cpp=%.o)
MAINOBJFILES = $(MAINOBJS:%.o=obj/%.o)

SRCS = $(patsubst source/%,%,$(wildcard source/*.cpp))
OBJS = $(SRCS:%.cpp=%.o)
OBJFILES = $(OBJS:%.o=obj/%.o)

INCLUDES := -Iinclude $(INCLUDES)

# For make depend:

ALLSRCS = $(wildcard main/*.cpp source/*.cpp)

.PHONY: test rpm

rpmsourcedir = /tmp/$(shell whoami)/rpmbuild

# The rules

all: objdir $(MAINPROGS)
debug: objdir $(MAINPROGS)
release: objdir $(MAINPROGS)
profile: objdir $(MAINPROGS)

$(MAINPROGS): % : $(OBJS) %.o
	$(CC) $(LDFLAGS) -o $@ obj/$@.o $(OBJFILES) $(LIBS)

clean:
	rm -f $(PROG) $(OBJFILES) *~ source/*~ include/*~

install:
	@mkdir -p $(bindir)
	$(INSTALL_PROG) $(BINNAME) $(bindir)

depend:
	gccmakedep -fDependencies -- $(CFLAGS) $(INCLUDES) -- $(ALLSRCS)

objdir:
	@mkdir -p $(objdir)
	@mkdir -p $(libdir)

rpm:    clean
	mkdir -p $(rpmsourcedir) ; \
        if [ -e $(PROG).spec ]; then \
          tar -C .. --exclude .svn -cf $(rpmsourcedir)/$(PROG).tar $(PROG) ; \
          gzip -f $(rpmsourcedir)/$(PROG).tar ; \
          rpmbuild -ta $(rpmsourcedir)/$(PROG).tar.gz ; \
          rm -f $(rpmsourcedir)/$(PROG).tar.gz ; \
        else \
          echo $(rpmerr); \
        fi;


.SUFFIXES: $(SUFFIXES) .cpp

.cpp.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $(objdir)/$@ $<

-include Dependencies
