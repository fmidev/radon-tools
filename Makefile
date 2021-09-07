PROG = radon-tools

SCONS_FLAGS=-j $(shell nproc)

# How to install

INSTALL_PROG = install -m 755

# rpm variables

CWP = $(shell pwd)
BIN = $(shell basename $(CWP))

rpmsourcedir = /tmp/$(shell whoami)/rpmbuild

INSTALL_TARGET = /usr/bin

# The rules

all release: 
	scons-3 $(SCONS_FLAGS)
debug: 
	scons-3 $(SCONS_FLAGS) --debug-build

clean:
	scons-3 -c ; scons-3 --debug-build -c ; rm -f *~ source/*~ include/*~

rpm:    clean
	mkdir -p $(rpmsourcedir) ; \
        if [ -a $(PROG).spec ]; \
        then \
          tar -C ../ --exclude .svn \
                   -cf $(rpmsourcedir)/$(PROG).tar $(PROG) ; \
          gzip -f $(rpmsourcedir)/$(PROG).tar ; \
          rpmbuild -ta $(rpmsourcedir)/$(PROG).tar.gz ; \
          rm -f $(rpmsourcedir)/$(LIB).tar.gz ; \
        else \
          echo $(rpmerr); \
        fi;

install:
	mkdir -p $(bindir)
	$(INSTALL_PROG) build/release/grid_to_radon $(bindir)

	if [ $(shell grep -ic suse /etc/issue) -eq 0 ]; then \
		$(INSTALL_PROG) python/radon_tables.py $(bindir) ; \
		$(INSTALL_PROG) python/previ_to_radon.py $(bindir) ; \
		$(INSTALL_PROG) python/geom_to_radon.py $(bindir) ; \
		$(INSTALL_PROG) python/calc_hybrid_level_height.py $(bindir) ; \
	fi;

test:	debug
	cd regression && sh test_all.sh
