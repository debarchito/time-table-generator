## 1. Setup

Install uv with the standalone installers:

```sh
# On macOS and Linux.
curl -LsSf https://astral.sh/uv/install.sh | sh
```

or

```pwsh
# On Windows.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Now, create a virtual environment `.venv` using:

```sh
uv venv --python 3.13
```

This will either reuse the Python version available on your system or if
unavailable, download Python for you. Subsequent use of `uv venv` will activate
the virtual environment. After activation, install the dependencies using:

```sh
uv sync --active
```
