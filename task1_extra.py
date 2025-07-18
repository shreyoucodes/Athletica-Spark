from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys

spark = SparkSession.builder \
    .appName("Olympics Analysis") \
    .getOrCreate()

athletes_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
athletes_2016 = spark.read.csv(sys.argv[2], header=True, inferSchema=True)
athletes_2020 = spark.read.csv(sys.argv[3], header=True, inferSchema=True)

coaches = spark.read.csv(sys.argv[4], header=True, inferSchema=True)
medals = spark.read.csv(sys.argv[5], header=True, inferSchema=True)

#athletes_2012.show()
#athletes_2016.show()
#athletes_2020.show()
#coaches.show()
#medals.show()

#2012
joined_2012 = athletes_2012.alias("a").join(
    medals.alias("m"),
    athletes_2012["id"] == medals["id"],
    "left"
).select(
    "a.id",  # Use the 'id' from athletes DataFrame
    "a.name",
    "a.sport",
    "m.medal",  # Use the 'medal' from medals DataFrame
)

joined_2012 = joined_2012.withColumn("points_2012", 
                                     F.when(joined_2012.medal == 'gold', 20)
                                      .when(joined_2012.medal == 'silver', 15)
                                      .when(joined_2012.medal == 'bronze', 10)
                                      .otherwise(0))
joined_2012.show()

athletes_points_2012 = joined_2012.groupBy("id", "name", "medal") \
    .agg(F.sum("points_2012").alias("total_points_2012"))

athletes_points_2012.show()

#2016   
joined_2016 = athletes_2016.alias("a").join(
    medals.alias("m"),
    athletes_2016["id"] == medals["id"],
    "left"
).select(
    "a.id",  # Use the 'id' from athletes DataFrame
    "a.name",
    "a.sport",
    "m.medal",  # Use the 'medal' from medals DataFrame
)

joined_2016 = joined_2016.withColumn("points_2016", 
                                     F.when(joined_2016.medal == 'gold', 12)
                                      .when(joined_2016.medal == 'silver', 8)
                                      .when(joined_2016.medal == 'bronze', 6)
                                      .otherwise(0))
joined_2016.show()
athletes_points_2016 = joined_2016.groupBy("id", "name", "sport","medal") \
    .agg(F.sum("points_2016").alias("total_points_2016"))

athletes_points_2016.show()

#2020
joined_2020 = athletes_2020.alias("a").join(
    medals.alias("m"),
    athletes_2020["id"] == medals["id"],
    "left"
).select(
    "a.id",  # Use the 'id' from athletes DataFrame
    "a.name",
    "a.sport",
    "m.medal",  # Use the 'medal' from medals DataFrame
)

# 2. Add the points_2016 column based on the type of medal
joined_2020 = joined_2020.withColumn("points_2020", 
                                     F.when(joined_2020.medal == 'gold', 15)
                                      .when(joined_2020.medal == 'silver', 12)
                                      .when(joined_2020.medal == 'bronze', 7)
                                      .otherwise(0))
joined_2020.show()
# 3. Group by athlete's ID, name, and sport to calculate total points for 2016
athletes_points_2020 = joined_2020.groupBy("id", "name", "sport", "medal") \
    .agg(F.sum("points_2020").alias("total_points_2020"))

# Show the result to ensure correctness
athletes_points_2020.show()

total_points = total_points.withColumn("total_points",
                                        F.col("total_points_2012") +
                                        F.col("total_points_2016") +
                                        F.col("total_points_2020"))

#sorted_athletes = total_points.orderBy(F.col("total_points").desc())
# Stop the Spark session


#sorted_athletes.write.csv(sys.argv[6], header=True, mode="overwrite")

spark.stop()
