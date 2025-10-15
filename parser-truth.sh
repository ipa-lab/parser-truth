#!/bin/bash

python3 -m venv .venv
source .venv/bin/activate

cd dataset_population/
python3 -m pip install -r requirements.txt
python3 ./populate_db.py truthUser

cd ../web-app/
python3 -m pip install -r requirements.txt
streamlit run app.py --server.baseUrlPath parser-truth --server.port 8501
