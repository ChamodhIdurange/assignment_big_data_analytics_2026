# Olist PySpark Project

## Prerequisites

- Python 3.10+ (recommended)
- Java 11 or 17 (required by PySpark)

## Setup

```bash
cd <project-root>   # folder that contains data/, src/, requirements.txt
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Place the original Olist CSV files under `data/raw/`.

## Run

From the project root (the folder that contains `src/`):

```bash
cd src
python main.py
```

Or:

```bash
PYTHONPATH=src python src/main.py
```

Outputs are written under `outputs/data/` and `outputs/visualizations/`. Final submission assets go in `deliverables/`.
