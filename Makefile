# Citus toplevel Makefile

REPO ?= hydra/columnar
TAG ?= latest
.PHONY: docker_build
docker_build:
	docker build -t $(REPO):$(TAG) .

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

all: extension

# build extension
extension: $(citus_top_builddir)/src/include/citus_version.h
	$(MAKE) -C src/backend/columnar all
install-extension: extension
	$(MAKE) -C src/backend/columnar install
install-headers: extension
	$(INSTALL_DATA) $(citus_top_builddir)/src/include/citus_version.h '$(DESTDIR)$(includedir_server)/'

clean-extension:
	$(MAKE) -C src/backend/columnar/ clean

.PHONY: extension install-extension clean-extension

# Add to generic targets
install: install-extension install-headers

install-downgrades:
	$(MAKE) -C src/backend/distributed/ install-downgrades
	
install-all: install-headers
	$(MAKE) -C src/backend/columnar/ install-all

clean: clean-extension

# apply or check style
reindent:
	${citus_abs_top_srcdir}/ci/fix_style.sh
check-style:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet --check
.PHONY: reindent check-style

# depend on install-all so that downgrade scripts are installed as well
check: all install-all
	$(MAKE) -C src/test/regress check-full

.PHONY: all check clean install install-downgrades install-all
