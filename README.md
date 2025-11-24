# Project virtual environment

This project uses a Python virtual environment located at `.venv`.

## Create the venv (if not already created)

```zsh
cd "/Volumes/Desctop_SSD/Skytap"
python3 -m venv .venv
```

## Activate (macOS / zsh)

```zsh
source .venv/bin/activate
# or: . .venv/bin/activate
```

After activation your prompt will change and `python` / `pip` will point to the venv.

## Deactivate

```zsh
deactivate
```

## Install dependencies

If the project has a `requirements.txt`, install with:

```zsh
pip install -r requirements.txt
```

## Quick check

```zsh
python --version
pip --version
```

If you'd like, I can create a `requirements.txt`, pin common packages, or set up a `pyproject.toml`/`venv`-based workflow next.
