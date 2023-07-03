# Steup
```sh
docker build . -f Dockerfile --pull --tag my-image:0.0.7
```

Change the airflow image version in compose.

```sh
docker-compose up airflow-init
```

```sh
docker-compose up
```
### In notebook container
```sh
pip install tweepy==3.10.0
pip install pyspark==3.1.2
```

### In Jupyter container
```
# Exec Root in Container
$ docker exec -u 0 -it airflow-standalone-jupyter-spark-1 bash
$ apt-get update
$ apt-get install gcc

# Exec airflow-standalone-jupyter-spark-1 Container without root
$ java -version
# should be Java 8 (Oracle or OpenJDK)
$ conda create -n sparknlp python=3.8 -y
$ conda activate sparknlp
$ pip install spark-nlp==4.0.2 pyspark==3.2.1
$ pip install tweepy==3.10.0
$ pip install tensorflow-aarch64
$ pip install ipykernel
```