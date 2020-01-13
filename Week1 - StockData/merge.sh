#!/bin/bash

echo "Date,High,Low,Open,Close,Volume,Adj Close,Name" > all_stocks.csv
#cd individual_stocks_5yr
files=$(ls ./stocks/*.csv)
for file in $files
do
	tail -n +2 $file >> all_stocks.csv
done