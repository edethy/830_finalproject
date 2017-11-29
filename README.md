How to generate a graph from SNAP.json
=========

>_ ant
>_ java -classpath dist/simpledb.jar simpledb.test
>_ java -jar dist/simpledb.jar convert [NAME_OF_NODE_TXT_FILE.txt] 6 "int,int,int,int,string,int" 
>_ java -jar dist/simpledb.jar convert [NAME_OF_EDGE_TXT_FILE.txt] 2 "int,int"
>_ java -classpath dist/simpledb.jar simpledb.GraphTable