#CloudSort benchmark 


This is an Spark program for running CloudSort benchmarks. It is based on
work from [Ewan Higgs's branch](https://github.com/ehiggs/spark-terasort) and [Dongwon Kim](https://github.com/eastcirclek/terasort)

## Generate data

    ./bin/spark-submit --class spark.terasort.TeraGen 
    path/to/spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 
    100g [ouput directory]

## Sort the data
    ./bin/spark-submit --class spark.terasort.TeraSort
    path/to/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 
    [input directory] [output directory] [partition num] [number of sampling records][ number of sampling threads]

 when input values for [partition num], [number of sampling records] and [number of sampling threads] are negative numbers, the program would take default values
 
## Validate the data
    ./bin/spark-submit --class spark.terasort.TeraValidate
    path/to/spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 
    [input directory]






