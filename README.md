# Flight Price Prediction

## Requirements
- JDK 11
- Scala 2.12.8
- Spark 3.2.1


## How to run
```shell
$ sbt run
$ # if you want to change default jdk version for sbt
$ # sbt --java-home <path/to/jdk> run 
Multiple main classes detected. Select one to run:
 [1] com.neu.edu.FlightPricePrediction.demo
 [2] com.neu.edu.FlightPricePrediction.pojo.FlightReader
 [3] com.neu.edu.FlightPricePrediction.predictor.FightPricePredictor
 [4] com.neu.edu.FlightPricePrediction.trainer.FlightPriceTrainer

Enter number: 
```

## Dataset

### Columns

- airline
- flight
- source_city
- departure_time
- stops
- arrival_time
- destination_city
- class
- duration float
- days_left int
- price int

#### Transform

- airline => onehot
- flight [ignored]
- source_city => onehot
- departure_time => onehot
- stops => onehot
- arrival_time => onehot
- destination_city => onehot
- class => numeric
- duration [float]
- days_left [int]
- price [int]
