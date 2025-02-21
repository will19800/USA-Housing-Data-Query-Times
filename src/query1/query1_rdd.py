from pyspark import SparkConf, SparkContext
import statistics

# Initialize Spark
conf = SparkConf().setAppName("Property Price Analysis by ZIP")
sc = SparkContext(conf=conf)

# Load the CSV file
# Assuming the header is in the first row
raw_data = sc.textFile("path_to_your_property_data.csv")
header = raw_data.first()
data = raw_data.filter(lambda line: line != header)

# Parse the CSV
# Assuming ZIP code is the 5th column (index 4) and price is the 9th column (index 8)
# Adjust these indices based on your actual data format
def parse_line(line):
    values = line.split(',')
    try:
        zip_code = values[4].strip()
        price = float(values[8].strip())
        return (zip_code, price)
    except (ValueError, IndexError):
        return None

parsed_data = data.map(parse_line).filter(lambda x: x is not None)

# Calculate average price by ZIP code
# 1. Create key-value pairs of (zip_code, (price, 1))
# 2. Reduce by key to sum prices and counts
# 3. Calculate average
sum_count = parsed_data.mapValues(lambda price: (price, 1)) \
                       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                       .mapValues(lambda sum_count: sum_count[0] / sum_count[1])

avg_prices_by_zip = sum_count.collect()

# Calculate median price by ZIP code
# Group prices by ZIP code and calculate median for each group
grouped_by_zip = parsed_data.groupByKey().mapValues(lambda prices: statistics.median(list(prices)))
median_prices_by_zip = grouped_by_zip.collect()

# Combine results
results = parsed_data.groupByKey() \
                    .mapValues(lambda prices: {
                        "count": len(list(prices)),
                        "avg_price": sum(prices) / len(list(prices)),
                        "median_price": statistics.median(list(prices))
                    }) \
                    .collect()

# Print results
for zip_code, stats in results:
    print(f"ZIP Code: {zip_code}")
    print(f"  Number of Properties: {stats['count']}")
    print(f"  Average Price: ${stats['avg_price']:,.2f}")
    print(f"  Median Price: ${stats['median_price']:,.2f}")
    print("-" * 40)

# Save results to a file
results_rdd = sc.parallelize(results)
results_rdd.saveAsTextFile("property_price_analysis_by_zip")

# Stop Spark context
sc.stop()