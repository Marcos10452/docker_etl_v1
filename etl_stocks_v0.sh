#!/bin/bash

clear 

line='----------------------------------------'

#------------Extact data ----------------------
PROC_NAME='Extract_data'
printf "%s %s [Running]\n" $PROC_NAME "${line:${#PROC_NAME}}"
docker exec -it worker1 /opt/spark/bin/spark-submit   --master 'spark://master:7077'    --jars /app/postgresql-42.1.4.jar /app/python/stocks/src/etl/Extract_data.py
printf "%s %s \033[32m[DONE]\033[0m\n " $PROC_NAME "${line:${#PROC_NAME}}"

file="$(pwd)"
file="${file}/dataset/stocks_aux/status.txt"
echo "${file}"
#echo $(< $file)
string="Database is already updated."
var=$(< $file)
test1=$var
#echo $test1
#echo $string

if [ "$test1" = "$string" ];
then
        echo "Database was updated stopped. "
else
        echo "Continue with Transformation and Load"
        #------------Transform data ----------------------
	PROC_NAME='Tranform_data'
	printf "%s %s [Running]\n" $PROC_NAME "${line:${#PROC_NAME}}"
	docker exec -it worker1 /opt/spark/bin/spark-submit   --master 'spark://master:7077'     /app/python/stocks/src/etl/Transform_data.py
	printf "%s %s \033[32m[DONE]\033[0m\n " $PROC_NAME "${line:${#PROC_NAME}}"


	#------------Load data ----------------------
	PROC_NAME='Load_data'
	printf "%s %s [Running]\n" $PROC_NAME "${line:${#PROC_NAME}}"
	docker exec -it worker1 /opt/spark/bin/spark-submit   --master 'spark://master:7077'    --jars /app/postgresql-42.1.4.jar /app/python/stocks/src/etl/Load_data.py
	printf "%s %s \033[32m[DONE]\033[0m\n " $PROC_NAME "${line:${#PROC_NAME}}"


	#echo -e "\e[1;31m This is red text \e[0m"
fi







