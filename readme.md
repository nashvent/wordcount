# Examen Hadoop wordcount
## Compilar 
```
javac WordCount.java -cp $(/usr/local/hadoop/bin/hadoop classpath)
```
## Pasar a .jar
```
jar cf WordCount.jar *.class
```

## Ejecutar con hadoop

```
hadoop jar WordCount.jar WordCount  textInput.txt output/
```


