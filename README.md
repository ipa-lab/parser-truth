# Ad-Hoc Parser Truth

A tool to analyze ad-hoc string parsers from python code snippets as part of the TYPES4STRINGS project.

It consists of a dabatase population script and a web app.
The population script takes a dataset of ad hoc parsers, consisting of a csv and a file structure of code snippets. As a result of this script a SQLite database is genereted/populated. For a detailed description refere to the [Populate DB](#populate-db) section.

The Streamlit web app visualizes the data and makes it searchable. The affiliation of projects to file and to the extracted code snippets is visualized in a tree like structure. Combined with the code and metadata information all at one glance. Further details on starting the web app can be found in the [Web App](#web-app) section.

This tool can be seen as a first iteration. With further development it can be used for annotating these ad hoc parsers with ground truth, resulting in a benchmark dataset, that can be used for analyzing parsing programs.

![Streamlit UI](assets/streamlit.png "Streamlit UI")

## Requirements

- Minimum Python Version `3.10`

## Populate DB

### Prerequisites

#### Initial User

An initial user must be set. This is a user that is saved in DB that can add datasets to the DB. This user needs to be configured in `config.py` before the database population script.

#### Dataset

- The source for the population script needs to be saved in the `data` folder, named `analysis_results.csv`. The csv needs to be saved directly under the `data` folder.

- wie soll das csv ausschauen
==TODO==

- The path for code of the parser slices needs to be specified in the CSV in a column called `file`. (snippets from the original method as the annotated code)

- The original code from the methods must be located under `data/ParserExamples/original_methods` folder.
==TODO==

### Dataset Population Script

- Parameter beschreiben
  - user

- nur wenn noch kein adhocparser.db file in data existiert werden Tabellen erstellt und initial User hinzugefügt

#### Run Commands

```bash
cd dataset_population/
```

```bash
pip install -r requirements.txt
```

```bash
python3 ./populate_db.py <importedBy>
```

![Database Diagram](assets/dbDiagram.png "Database Diagram")

## Web App

### Start Web App

```bash
cd web-app/
```

```bash
pip install -r requirements.txt
```

```bash
streamlit run app.py
```

or

```bash
python3 -m streamlit run app.py
```
