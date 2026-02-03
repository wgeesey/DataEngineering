
This is a README for how to set up your environment to work on exercises for this section

# 1. Create a Python virtual environment

To create a Python virtual environment run the following command:

```sh
python -m venv venv
```

Note that you might need to run it with a specific Python version to make sure it is compatible with the Airflow version you are using.

```sh
python3.12 -m venv venv
```

# 2. Activate a Python virtual environment

To activate a virtual environment run the following command:

```sh
source venv/bin/activate
```

# 3. Install Airflow

Install Airflow using these commands:

```sh
AIRFLOW_VERSION="2.10.4"
# Set this variable to your Python version
PYTHON_VERSION="3.12"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"
```

Not all Airflow versions are compatible with all Python versions.

You can find a list of compatible versions on this page: https://pypi.org/project/apache-airflow/

# 4. Create an Airflow database

Run this command to create an Airflow database

```sh
airflow db migrate
```

# 5. Create a folder for DAGs

Create a folder for the DAGs you will implement

```sh
mkdir dags
```

# 6. Update Airflow configuration

To update the Airflow configuration get the location of the Airflow's home directory. To do this first run the following command.

```sh
airflow info
```

And after this update edit the Airflow configuration.

```sh
vim <airflow-home-path>/airflow.cfg
```

In this configuration file you would need to change two values:

* `dags_folder` to the path to the `dags` folder you've just created
* `load_examples` to `False`


# 7. Create an Airflow user

```sh
airflow users create \
  --username admin \
  --firstname <your-name> \
  --lastname <your-surname> \
  --role Admin \
  --email admin@example.com \
  --password admin
```


# 8. Start a web server for Airflow

To start a web server run the following command:

```sh
airflow webserver --port 8080
```

# 9. Start a scheduler

In a different terminal session, activate a virtual environment in the same folder and start an Airflow scheduler

```sh
source venv/bin/activate
airflow scheduler
```

# 10. Check if Airflow is working

You should now be able to go to `localhost:8080`, and log into Airflow UI. You should you the username and password you've selected in step `7`.
