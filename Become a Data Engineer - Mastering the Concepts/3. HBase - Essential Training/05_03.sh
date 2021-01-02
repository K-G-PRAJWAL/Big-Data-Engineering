# login to server
ssh root@127.0.0.1 -p 2222

# switch to hbase user
su hbase

# launch hbase shell
hbase shell

# check basic hbase commands
hbase(main):001:0> help
hbase(main):004:0> version
hbase(main):004:0> whoami
hbase(main):004:0> status 'summary'
hbase(main):004:0> status 'simple'
