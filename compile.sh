javac WordCount.java -cp $(/usr/local/hadoop/bin/hadoop classpath) &&
jar cf WordCount.jar *.class