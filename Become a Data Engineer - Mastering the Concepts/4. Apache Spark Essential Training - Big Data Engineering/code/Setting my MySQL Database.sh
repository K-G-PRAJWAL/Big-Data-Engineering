#--------------------------------------------------------
#----setting up mysql with sample data 
#--------------------------------------------------------
#Create new mysql user
mysql -u root -pcloudera
GRANT ALL ON *.* to cloudera@'%' IDENTIFIED BY 'cloudera';
flush privileges;
exit;

#Login as user and create the sample table.
mysql -u cloudera -pcloudera

#Execute the following commands within mysql

create database jdbctest;
use jdbctest;

CREATE TABLE jdbctest.jdbc_source (
   TEST_ID INT AUTO_INCREMENT,
   `TEST_TIMESTAMP` TIMESTAMP,
  PRIMARY KEY (TEST_ID)
) ENGINE = InnoDB ROW_FORMAT = DEFAULT;

#Use this command to create any number of records in the table.
insert into jdbc_source(TEST_TIMESTAMP) values ( now());
insert into jdbc_source(TEST_TIMESTAMP) values ( now());
insert into jdbc_source(TEST_TIMESTAMP) values ( now());
insert into jdbc_source(TEST_TIMESTAMP) values ( now());

create database exec_reports;
use exec_reports;

CREATE TABLE exec_reports.exec_summary (
   ID INT AUTO_INCREMENT NOT NULL,
   REPORT_DATE DATETIME NOT NULL,
   SALES DOUBLE NOT NULL DEFAULT '0',
   WEB_HITS INT NOT NULL DEFAULT '0',
   TWEETS INT NOT NULL DEFAULT '0',
   TWEETS_POSITIVE INT NOT NULL DEFAULT '0',
  PRIMARY KEY (ID),
  CONSTRAINT UC_datehour UNIQUE (REPORT_DATE)
) ENGINE = InnoDB ROW_FORMAT = DEFAULT;

INSERT INTO exec_reports.exec_summary
(REPORT_DATE) VALUES (date_format(now(), '%Y-%m-%d %H:00:00'));

create database us_sales;
use us_sales;

CREATE TABLE `garment_sales` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `GARMENT_NAME` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  SALES_DATE DATETIME NOT NULL,
  `ORDER_VALUE` double DEFAULT '0',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO us_sales.garment_sales
(GARMENT_NAME, SALES_DATE, ORDER_VALUE) 
VALUES ('Mens Pants', now(), 45.99);

INSERT INTO us_sales.garment_sales
(GARMENT_NAME, SALES_DATE, ORDER_VALUE) 
VALUES ('Womens Tops',now(), 29.99);



create database eu_sales;
use eu_sales;

CREATE TABLE `book_sales` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `BOOK_NAME` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  SALES_DATE DATETIME NOT NULL,
  `ORDER_AMOUNT` double DEFAULT '0',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO eu_sales.book_sales
(BOOK_NAME, SALES_DATE, ORDER_AMOUNT) 
VALUES ('Java Programming', now(),25.00);

INSERT INTO eu_sales.book_sales
(BOOK_NAME, SALES_DATE, ORDER_AMOUNT) 
VALUES ('Spark Guide',now(), 32.00);





