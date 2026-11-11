# data_engineer
Data Engineer projects snippets notes


Data files

* Bikes
Google Data Analytics Professional Certificate capstone (the Cyclistic case study) comes from a fictional Chicago bike-share company based on real data.
https://divvy-tripdata.s3.amazonaws.com/index.html

* flights
https://www.kaggle.com/datasets/tylerx/flights-and-airports-data
https://www.kaggle.com/datasets/mahoora00135/flights


## install pyspark
```
pip install pyspark[sql]
```


## new notebooks lab
```
 
python -m pip install --upgrade pip
pip install jupyterlab

jupyter lab
```



## how to run notebooks

goto your folder
and run jupyter

```
cd /home/art/git/data_engineer
jupyter notebook
```

## beam installation

```
pip install apache-beam
pip install "apache-beam[interactive]" graphviz notebook
pip install apache-beam beam-postgres-connector
pip install apache-beam beam-nugget psycopg2-binary

```

## delete checkpoint files
```
find . -type f -name "*checkpoint*" -delete
```

If they are directories (like .ipynb_checkpoints):
```
find . -type d -name "*checkpoint*" -exec rm -rf {} +
```
find . -type f -name "*.delta.crc" -delete
find . -type d -name "*checkpoint*" -exec rm -rf {} +


## unmask the shadows
```
find . -name "*.crc" -o -name "*checkpoint*"
```

## delete crc files
```
find . -name "*.crc" -o -name "*checkpoint*" -delete
```


