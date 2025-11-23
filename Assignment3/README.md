# Environment Setup

## Setup Benchmarking

1. [Follow this Guide to setup spark](https://spark.apache.org/downloads.html)

2. copy the .env.example file with `copy .env.example .env` and ensure proper settings

3. create virtual environment and activate:

   1. `py -m venv .venv`
   2. `venv\scripts\activate`
   3. `pip install -r requirements.txt`

4. Run file `py benchmark.py`

## Output

- Plots are inside /plot after running
- stored parquests are found in dataset_parquets
- both these folders are generated after running benchmark.py
