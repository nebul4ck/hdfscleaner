# HDFS Cleaner

## Add Saturn Repository

Take a look to the next link: https://wiki-wlabs.wtelecom.es/doku.php?id=wlabs:tech:buanarepo&s[]=buanarepo

## Install hdfscleaner

```
# apt install hdfscleaner-saturn
# hdfscleaner --help
```

## Configure hdfscleaner

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
