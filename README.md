# Ad-Hoc Parser Truth

==TODO: Description what is this App, mention Python, SQLite and Streamlit==

![Streamlit UI](assets/streamlit.png "Streamlit UI")

<details>
<summary>Requirements</summary>

## Python

Minimum Python Version 3.10
</details>

<details>
<summary>Populate DB</summary>

## Assumptions

- method code is the original code from dataset 'original_methods' folder, and slice code is the annotated code in the respective folders

## Dataset Population Script

- initial user erwähnen
- Parameter beschreiben
- wenn das populate script aufgerufen wird wo muss das csv liegen
- wie soll das csv ausschauen

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
