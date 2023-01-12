# Firebolt Airflow Provider Examples

This repo features two sample workflows used to sync an S3 repository as well
as update Firebolt data from JSON based data.

In order to have Apache Airflow up and running, this assumes you run Apache
Airflow with docker compose as mentioned in the [official
HOWTO](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

In order to create a custom docker image with the Firebolt provider installed,
you can run in this directory:

```
docker build . --tag airflow-firebolt-provider-2.5.0
```

This will install the Firebolt provider in the docker image first and then tag
the image properly.

Now you can run this in your directory containing docker compose:

```
AIRFLOW_IMAGE_NAME="airflow-firebolt-provider-2.5.0" docker compose up
```

Now your custom image is used and you can create a Firebolt connection in your
Apache airflow setup.

If you want to use the two supplied workflows (take your time to read them
first), simply copy them into the `dags` folder if you airflow directory and
you should be good to go!

