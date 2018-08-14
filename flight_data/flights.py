
from datetime import datetime
from collections import namedtuple
from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

airlines_path = "/home/atarasov/Documents/InstallGuideAndSourceCode/Datasets/data/airlines.csv"
flights_path = "/home/atarasov/Documents/InstallGuideAndSourceCode/Datasets/data/flights.csv"
airports_path = "/home/atarasov/Documents/InstallGuideAndSourceCode/Datasets/data/airports.csv"

airlines = sc.textFile(airlines_path)
flights = sc.textFile(flights_path)
airports = sc.textFile(airports_path)

airlineswoh = airlines.filter(lambda x: "Description" not in x)
airlines_parsed = airlineswoh.map(lambda x: x.split(","))

flights_parsed = flights.map(lambda x: x.split(","))

fields = ("date", "airline", "flightnum", "origin", "dest", "dep", "dep_delay",
          "arv", "arv_delay", "air_time", "distance")
Flight = namedtuple('Flight', fields)
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

flights_parsed.first()

def parse(row):
    row[0] = datetime.strptime(row[0], DATE_FMT).date()
    row[5] = datetime.strptime(row[5], TIME_FMT).time()
    row[6] = float(row[6])
    row[7] = datetime.strptime(row[7], TIME_FMT).time()
    row[8] = float(row[8])
    row[9] = float(row[9])
    row[10] = float(row[10])
    return Flight(*row)

flights_parsed = flights_parsed.map(parse)

flights_parsed.map(parse).first()

tottal_dist = flights_parsed.map(lambda x: x.distance).reduce(lambda x,y: x + y)
avg_dist = tottal_dist/flights.count()
avg_dist

flights_parsed.persist()

delate_flights = flights_parsed.map(lambda x: x.dep_delay > 0).count()
delate_percent = flights.count() / delate_flights * 100
delate_percent

sum_delays = flights_parsed.map(lambda x: x.dep_delay).aggregate(
(0, 0),
(lambda acc, val: (acc[0] + val, acc[1] + 1)),
(lambda acc1, acc2: (acc1[0] + acc2[0], acc2[1] + acc2[1])))
avg_delay = sum_delays[0] / sum_delays[1]
avg_delay

delay_distr = flights_parsed.map(lambda x: int(x.dep_delay/60)).countByValue()

sc.stop()
