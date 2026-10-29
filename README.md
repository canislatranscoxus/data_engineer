# data_engineer
Data Engineer projects snippets notes


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



