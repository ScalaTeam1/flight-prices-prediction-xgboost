# Steps

## Columns

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

### Transform

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
