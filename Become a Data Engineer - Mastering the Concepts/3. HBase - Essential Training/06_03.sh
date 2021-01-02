# import CSV file using ImportTsv MapReduce method
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.columns="\
HBASE_ROW_KEY, \
order:orderID, \
order:orderDate, \
order:shipDate, \
order:shipMode, \
order:profit, \
order:quantity, \
order:sales" sales hdfs://sandbox-hdp.hortonworks.com:/tmp/sales.csv

# check table after, from hbase shell
scan 'sales'

# add new row using put
put 'sales','5010','order:orderID','US-2014-169552'
put 'sales','5010','order:orderDate','2017-10-01'
put 'sales','5010','order:shipDate','2017-10-13 03:25:03.567'
put 'sales','5010','order:shipMode','Standard'
put 'sales','5010','order:profit','237.76'
put 'sales','5010','order:quantity','15'
put 'sales','5010','order:sales','745.93'

# see our new row
get 'sales','5010'

# fix the shipMode
put 'sales','5010','order:shipMode','Standard Class'

# verify our change
get 'sales','5010'
