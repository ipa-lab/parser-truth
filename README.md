# Ad-Hoc Parser Truth

A tool to analyze ad-hoc string parsers from python code snippets as part of the TYPES4STRINGS project.

It consists of a dabatase population script and a web app.
The population script takes a dataset of ad hoc parsers, consisting of a csv and a file structure of code snippets. As a result of this script a SQLite database is genereted/populated. For a detailed description refere to the [Populate DB](#prerequisites) section.

The Streamlit web app visualizes the data and makes it searchable. The affiliation of projects to file and to the extracted code snippets is visualized in a tree like structure. Combined with the code and metadata information all at one glance. Further details on starting the web app can be found in the [Web App](#start-web-app) section.

This tool can be seen as a first iteration. With further development it can be used for annotating these ad hoc parsers with ground truth, resulting in a benchmark dataset, that can be used for analyzing parsing programs.

![Streamlit UI](assets/streamlit.png "Streamlit UI")

<details>
<summary>Requirements</summary>

- Minimum Python Version `3.10`

</details>

<details>
<summary>Populate DB</summary>

## Prerequisites

- set the initial user (a user that is saved in db that can add datasets to the db)
- method code is the original code from dataset 'original_methods' folder, and slice code is the annotated code in the respective folders

## Dataset Population Script

- initial user erwähnen
- Parameter beschreiben
- wenn das populate script aufgerufen wird wo muss das csv liegen
  - wie soll das csv ausschauen
- und wo müssen die code snippets liegen, die rein geladen werden (in der DB abgespeichert werden)
  - wie soll der Ordner heißen und die Unterornder und wie sollen die Files benannt sein

- nur wenn noch kein adhocparser.db file in data existiert werden Tabellen erstellt und initial User hinzugefügt

### Run Commands

```bash
cd dataset_population/
```

```bash
pip install -r requirements.txt
```

```bash
python3 ./populate_db.py
```

![Database Diagram](assets/dbDiagram.png "Database Diagram")

</details>

<details>
<summary>Web App</summary>

## Start Web App

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

</details>
