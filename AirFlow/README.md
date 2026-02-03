## Create and Activate a Virtual Environment

Create a virtual environment using a supported Python version:

```sh
python -m venv venv
```

Activate the virtual environment:

```sh
source venv/bin/activate
```

Check the Python version to confirm you're using the right one:

```sh
python --version
```

---

## Install Apache Airflow

Airflow must be installed with a constraints file to ensure compatible dependencies.

```sh
pip install 'apache-airflow==2.10.4' \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"
```

---

## Initialize the Metadata Database

Set up the Airflow database:

```sh
airflow db migrate
```

---

## Create an Admin User

Create a user account for logging into the Airflow web UI:

```sh
airflow users create \
  --username admin \
  --firstname John \
  --lastname Doe \
  --role Admin \
  --email admin@example.com \
  --password admin
```

---

## Configure DAGs Folder and Disable Example DAGs

Find the Airflow home directory:

```sh
airflow info
```

Locate the `airflow_home` value in the output. The config file is at:

```
<airflow_home>/airflow.cfg
```

Get your current working directory (where you will store DAGs):

```sh
pwd
```

Edit the configuration file:

```sh
vim <airflow_home>/airflow.cfg
```

Update the following settings:

```ini
dags_folder = /your/current/directory
load_examples = False
```

Save and exit.

---

## Start Airflow Services

Start the Airflow webserver on port 8080:

```sh
airflow webserver --port 8080
```

In a new terminal with the virtual environment activated, start the scheduler:

```sh
airflow scheduler
```

---

## Access the Web UI

Open a browser and go to:

```
http://localhost:8080
```

Login credentials:

- **Username:** `admin`
- **Password:** `admin`

---

## Clean Up

To stop Airflow and exit the virtual environment:

1. Press `Ctrl+C` in both terminal windows to stop the webserver and scheduler.
2. Deactivate the virtual environment:

```sh
deactivate
```

Airflow is now installed and ready to use.