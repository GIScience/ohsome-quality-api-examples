# OQAPI Notebooks

Jupyter Notebook with examples on how to use the ohsome quality API with Python.

## Usage

To launch this notebook with Binder press this button: [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/GIScience/oqt-examples.git/HEAD?labpath=notebook.ipynb)

Requests to the ohsome quality API require an API key. Get a free key at
[account.heigit.org](https://account.heigit.org/) and set it as the
`OQAPI_API_KEY` environment variable before running the notebook or scripts.

## About

[Website](https://api.quality.ohsome.org) | [GitHub](https://github.com/GIScience/ohsome-quality-api) | [API docs](https://api.heigit.org/docs/?urls.primaryName=ohsome+quality+API)

## Development Setup

### Requirements

- Python: 3.13
- [uv](https://docs.astral.sh/uv/)

### Installation

```bash
uv sync
```

This creates a virtual environment at `.venv` and installs all dependencies,
pinned in `uv.lock`.

To also install the [pre-commit](https://pre-commit.com) hooks, run via
[prek](https://github.com/j178/prek):

```bash
uv run prek install
```

### Usage

```bash
uv run jupyter notebook
```
