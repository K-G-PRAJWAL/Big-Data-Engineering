# from ssh as root
# download data from github and store in sales.csv
curl -o ~/sales.csv https://raw.githubusercontent.com/bsullins/data/master/salesOrders.csv

# inspect our file
head sales.csv

# remove header row
sed -i '1d' sales.csv

# updload to hadoop
hadoop fs -copyFromLocal ~/sales.csv /tmp

# view file in hdfs
hadoop fs -ls /tmp
