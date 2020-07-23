# HDFS Cleaner

This service let us to delete all druid segments with key:value "user=0" in SQL data base.

## Install hdfscleaner

1. Clone this repository

```
$ git clone https://github.com/nebul4ck/hdfscleaner.git
```

2. Create a debian package

```
$ dpkg -b hdfscleaner/ hdfscleaner-0.1.0.deb
```

3. Install debian package

```
$ sudo apt install ./hdfscleaner-0.1.0.deb
```

4. Use the help

```
$ hdfscleaner --help
```

5. Remove hdscleaner

```
$ sudo apt remove hdfscleaner
```

## hdfscleaner settings

```
# vi /etc/hdfscleaner/conf.d/druidConf.py
```
**Note:** A crontab task in root user is added automatically

## Modules

### Druid Cleaner

Nowadays all segments with "used = 0" in druid database will be removed.

Use: # hdfscleaner druid

TODO: custom segments deleting

### Spark Cleaner

Delete old Spark checkpoints.
