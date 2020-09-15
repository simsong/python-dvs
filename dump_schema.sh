#!/bin/bash

source $HOME/dbreader.bash
DVS_TABLES=$(echo 'show tables like "dvs%"' |  mysql -u$MYSQL_USER -p$MYSQL_PASSWORD -h$MYSQL_HOST $MYSQL_DATABASE|grep dvs_)
mysqldump -d --single-transaction=TRUE --max-allowed-packet=1g -u$MYSQL_USER -p$MYSQL_PASSWORD -h$MYSQL_HOST $MYSQL_DATABASE $DVS_TABLES


