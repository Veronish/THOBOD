CREATE TABLE IF NOT EXISTS Logs ( 
ip string, 
trash1 string,
trash2 string, 
trash3 string, 
trash4 string, 
trash5 string,
trash6 string, 
trash7 string, 
trash8 string, 
byte int)

COMMENT 'Byte sum' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' STORED 
AS TEXTFILE;
LOAD DATA LOCAL INPATH '/opt/hive/examples/sample.txt' OVERWRITE INTO TABLE Logs;

SELECT * FROM Logs;
CREATE TABLE IF NOT EXISTS Statistic (ip string, sumByte int, avgByte int);
INSERT INTO Statistic (ip, sumbyte, avgbyte)
SELECT ip, SUM(byte) as sumByte, AVG(byte) as avgByte
FROM Logs
GROUP BY ip;
SELECT * FROM Statistic;