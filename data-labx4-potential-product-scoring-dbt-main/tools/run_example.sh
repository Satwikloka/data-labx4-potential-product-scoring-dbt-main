#!/bin/sh
cd /usr/app/dbt
dbt run --profiles-dir $(pwd) --select marts.example.* 
exit $?
