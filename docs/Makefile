# Global variables
UV              = uv
SPHINXOPTS      = -j auto
SPHINXBUILD     = $(UV) run sphinx-build
SPHINXAUTOBUILD = $(UV) run sphinx-autobuild
BUILDDIR        = _build
SOURCEDIR       = source
PORT            ?= 5500
HOST            ?= 127.0.0.1

ALLSPHINXOPTS = -d $(BUILDDIR)/doctrees $(SPHINXOPTS) $(SOURCEDIR)

.PHONY: all
all: dirhtml

# Setup commands
.PHONY: setup
setup:
	$(UV) sync

.PHONY: update
update:
	$(UV) sync --upgrade

# Clean commands
.PHONY: pristine
pristine: clean
	git clean -dfX

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)

# Standard HTML builder.
.PHONY: html
html: setup
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

# Directory HTML builder.
.PHONY: dirhtml
dirhtml: setup
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

# Live preview.
# Set PORT to choose the preview port (default: 5500), e.g. `make preview PORT=8000`.
# Set HOST to choose the preview host (default: 127.0.0.1), e.g. `make preview HOST=0.0.0.0`.
.PHONY: preview
preview: setup
	$(SPHINXAUTOBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml --port $(PORT) --host $(HOST)

# Fail on warnings, matching the theme's PR checks.
.PHONY: test
test: setup
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) -W --keep-going $(BUILDDIR)/dirhtml

.PHONY: linkcheck
linkcheck: setup
	$(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck

# Build the configured Git refs for publication.
.PHONY: multiversion
multiversion: setup
	$(UV) run sphinx-multiversion $(SOURCEDIR) $(BUILDDIR)/dirhtml -W --keep-going

.PHONY: multiversionpreview
multiversionpreview: multiversion
	$(UV) run python -m http.server $(PORT) --bind $(HOST) --directory $(BUILDDIR)/dirhtml
