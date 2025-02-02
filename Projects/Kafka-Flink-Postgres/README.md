- Project reads from a kafka-producer that produces some dummy weather data and gets processed by flink to be loaded to Postgres DB
- Build maven package from the flink-processor directory: `mvn clean package`
- Docker compose up using `docker-compose up -d`
- Check the output in postgres container as follows:

```
$ docker exec -it postgres psql -U postgres -d postgres
psql (17.2 (Debian 17.2-1.pgdg120+1))
Type "help" for help.

postgres=# \dt
          List of relations
 Schema |  Name   | Type  |  Owner
--------+---------+-------+----------
 public | weather | table | postgres
(1 row)

postgres=# select * from weather;
 id |         city          | average_temperature
----+-----------------------+---------------------
  1 | Autumnhaven           |   18.89089610202364
  2 | Fernandezmouth        |  10.802756954055706
 14 | Lake Dustinport       |   33.21463634564876
  3 | Theresaborough        |  19.199319978076915
  5 | Willisbury            |   94.90272798548233
  4 | Port Gregorymouth     |    88.0836203877942
  6 | North Stephen         |  60.431946849961115
  7 | Sherriborough         |  27.255258851821342
  8 | New Stacey            |  61.948338610765774
  9 | Foleyburgh            |   56.07583584314513
 10 | Port Charles          |  15.470469864170157
 11 | South Sharon          |      80.80588135423
 12 | Tuckermouth           |  49.060241738298906
 13 | Port Jeremiah         |   53.41605012720401
 15 | North Kenneth         |   100.0569444003501
 16 | Donnafurt             |  32.857736232470046
 17 | East Jamesland        |  15.355453449094693
 18 | East Christopherhaven |   76.23572961091617
 19 | New Aliciafort        |  16.994575497699994
(19 rows)
```
