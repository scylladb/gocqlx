from __future__ import annotations

from sphinx_scylladb_theme.utils import (  # pyright: ignore[reportMissingTypeStubs]
    multiversion_regex_builder,  # pyright: ignore[reportUnknownVariableType]
)

# -- Global variables --------------------------------------------------

# Publish only the default branch for now, under the stable URL.
TAGS: list[str] = []
BRANCHES = ["master"]
LATEST_VERSION = "master"
UNSTABLE_VERSIONS = []
DEPRECATED_VERSIONS = []

# -- General configuration ---------------------------------------------

extensions = [
    "sphinx.ext.githubpages",
    "sphinx_sitemap",
    "sphinx_multiversion",
    "myst_parser",
    "sphinx_scylladb_theme",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

master_doc = "index"

project = "GocqlX"
copyright = "2026, ScyllaDB"
author = "ScyllaDB"

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "**/_partials",
]

# -- Options for myst parser -------------------------------------------

myst_enable_extensions = ["colon_fence"]
myst_heading_anchors = 3

# -- Options for not found extension -----------------------------------

# Use the theme's 404 page with assets served from the custom domain root.
notfound_template = "404.html"
notfound_urls_prefix = ""

# -- Options for sitemap extension -------------------------------------

sitemap_url_scheme = "/stable/{link}"

# -- Options for multiversion extension --------------------------------

smv_tag_whitelist = multiversion_regex_builder(TAGS)
smv_branch_whitelist = multiversion_regex_builder(BRANCHES)
smv_latest_version = LATEST_VERSION
smv_rename_latest_version = "stable"
smv_remote_whitelist = r"^origin$"
smv_released_pattern = r"^tags/.*$"
smv_outputdir_format = "{ref.name}"

# -- Options for HTML output -------------------------------------------

html_theme = "sphinx_scylladb_theme"

html_theme_options = {  # type: ignore
    "conf_py_path": "docs/source/",
    "github_repository": "scylladb/gocqlx",
    "github_issues_repository": "scylladb/gocqlx",
    "default_branch": "master",
    "site_description": "GocqlX: Go tools for working with ScyllaDB using the gocql driver.",
    "hide_edit_this_page_button": "false",
    "hide_feedback_buttons": "false",
    "hide_version_dropdown": [],
    "versions_unstable": UNSTABLE_VERSIONS,
    "versions_deprecated": DEPRECATED_VERSIONS,
}

html_title = "GocqlX Documentation"
html_short_title = "GocqlX"
html_sidebars = {"**": ["side-nav.html"]}

# Root URL for the multiversion GitHub Pages deployment.
html_baseurl = "https://gocqlx-driver.docs.scylladb.com"
html_context = {"html_baseurl": html_baseurl}
