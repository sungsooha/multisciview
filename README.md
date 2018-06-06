# multisciview

## Dependency
* flask
* pymongo
* numpy
* pillow
* watchdog

## Installation with anaconda
* `conda install -n <env> -c anaconda flask`
* `conda install -n <env> -c anaconda pymongo`
* `conda install -n <env> -c anaconda numpy`
* `conda install -n <env> -c anaconda pillow`
* `conda install -n <env> -c conda-forge watchdog`

## Run
It first requires to start mongo daemon or acquire host address and port number in advance.
Then, [DB] fields in db/db_config.py must be modifed according to it. After that, you are good to go by typing:
```
>> python app.py -s <web server host> -p <web server port> -r <root directory in a local filesystem>
```

