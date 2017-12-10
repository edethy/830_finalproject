How to generate a graph from SLAP.json
=========

ant <br />
java -classpath dist/simpledb.jar simpledb.test <br />
java -jar dist/simpledb.jar convert [NAME_OF_NODE_TXT_FILE.txt] 6 "int,int,int,int,string,int" <br />
java -jar dist/simpledb.jar convert [NAME_OF_EDGE_TXT_FILE.txt] 4 "int,int,int,int" <br />
java -classpath dist/simpledb.jar simpledb.GraphTable <br />
