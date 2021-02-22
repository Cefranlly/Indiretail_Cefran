# Indiretail_Cefran

Proyecto Spark para el calculo de métricas de negocios en el ecosistema de retailers.

## Table Of Contents
- [Project structure](#Project structure)
- [Prerequisites](#Prerequisites)
- [Setup environment](#Setup environment)
- [How to run it?](#How to run it)
- [How to test it?](#How to test?)
- [Contributing](#Contributing)


## Project structure

```
├── main.py                 # Principal execution file.
├── pyspark-test.ipynb      # Jupyter notebook with data exploration.
├── lib
│   └── utils               # Dir that centralizes python/spark utils for project development.
│       └── utils.py        # Dags helper module.         
│   └── logger              # Dir for logs classes.
│       └── logger.py       # Class for logger declaration.
│   └── sparkconfig         # Dir for Apache spark configuration
│       └── utils.py        # Spark configuration loader.
├── data                    # Dir to put data files.
│   └── RESULT_DIRS         # Dirs for results.
│   └── *.parquet           # Input files.
├── app-logs                # Dir to put log files.
│   └── spark-exec.log      # Log file.
├── test                    # Dir to put testing cases.
│   └── testing.py          # File with test cases.
├── log4j.properties        # Log configuration file.
└── spark.conf              # File that centralizes configurations.
```

## Prerequisites

- python (Version ~> 3)
- pip
- pyspark
- jupyter notebook (es un plus)

## Setup environment

``here we put our setup environment instructions``

## How to run it

``here we put our running steps``

## How to test?

``here we put testing instructions``

## Contributing

``her we include reference to contribution pipeline guide``
