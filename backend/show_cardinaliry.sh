#!/bin/bash

function cardinality(){
  table_name=$1
  echo ""
  echo ""
  echo "=================== ${table_name} ================================"
  columns=$(mysql -D tpch -uroot -proot -se "SELECT column_name FROM information_schema.columns WHERE table_name = \"${table_name}\"")
  column_array=(`echo $columns | sed 's/ /\n/g'`)
  for c in "${column_array[@]}"
  do
	  echo "-- ${c} --"
	  mysql -D tpch -uroot -proot -se "SELECT COUNT(DISTINCT(${c})) FROM ${table_name}"

  done
}

cardinality lineitem
cardinality orders 
cardinality customer
cardinality part
cardinality partsupp
cardinality nation
cardinality region
