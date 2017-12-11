How to generate a graph from SLAP.json
=========

1. Convert the .json to .txt <br />
ant <br />
java -classpath dist/simpledb.jar simpledb.test [NAME_OF_NODE_TXT_FILE.txt] [NAME_OF_EDGE_TXT_FILE.txt]<br />

2. Convert the .txt to .dat <br />
java -jar dist/simpledb.jar convert [NAME_OF_NODE_TXT_FILE.txt] 6 "int,int,int,int,string,int" <br />
java -jar dist/simpledb.jar convert [NAME_OF_EDGE_TXT_FILE.txt] 4 "int,int,int,int" <br />
java -classpath dist/simpledb.jar simpledb.GraphTable <br />