#!/usr/bin/env python3

#SHREYA KIRAN PES1UG22CS571
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, sum, count, desc, row_number, lit, asc
from pyspark.sql.window import Window
import sys

def init_spark():
    return SparkSession.builder.appName("OlympicAthCoachAnalysis").getOrCreate()

def read_csv(spark, fpath):
    return spark.read.csv(fpath,header=True,inferSchema=True)

def upper_data(df):
    return df.select([upper(col(c)).alias(c) for c in df.columns])

def year_add(df, year):
    return df.withColumn("YEAR",lit(year))

def filter_years(df, col_name):
    if col_name in df.columns:
        return df.filter(col(col_name).isin(["2012","2016","2020"]))
    else:
        return df

def calc_medal_pt(medals_df):
    medals_df=upper_data(medals_df)
    return medals_df.withColumn("POINTS", 
        when((col("YEAR") == "2012") & (col("MEDAL") == "GOLD"), 20)
        .when((col("YEAR") == "2012") & (col("MEDAL") == "SILVER"), 15)
        .when((col("YEAR") == "2012") & (col("MEDAL") == "BRONZE"), 10)
        .when((col("YEAR") == "2016") & (col("MEDAL") == "GOLD"), 12)
        .when((col("YEAR") == "2016") & (col("MEDAL") == "SILVER"), 8)
        .when((col("YEAR") == "2016") & (col("MEDAL") == "BRONZE"), 6)
        .when((col("YEAR") == "2020") & (col("MEDAL") == "GOLD"), 15)
        .when((col("YEAR") == "2020") & (col("MEDAL") == "SILVER"), 12)
        .when((col("YEAR") == "2020") & (col("MEDAL") == "BRONZE"), 7)
        .otherwise(0))

def get_best_ath(athletes_df, medals_df):
    athletes_df=upper_data(athletes_df)
    medals_df=upper_data(medals_df)
    medal_points=calc_medal_pt(medals_df)
    #medal_points.show()
    
    athlete_points=medal_points.groupBy("id", col("sport").alias("medal_sport")).agg(
        sum("points").alias("total_points"),
        count(when(col("medal")=="GOLD",True)).alias("gold_count"),
        count(when(col("medal")=="SILVER",True)).alias("silver_count"),
        count(when(col("medal")=="BRONZE",True)).alias("bronze_count")
    )
    #athlete_points.show()
    joined_df=athletes_df.join(athlete_points,athletes_df.id==athlete_points.id).drop(athlete_points.id)
    #joined_df.show()
    
    window_best_ath=Window.partitionBy(joined_df.sport).orderBy(desc("TOTAL_POINTS"), desc("GOLD_COUNT"), 
                                                              desc("SILVER_COUNT"), desc("BRONZE_COUNT"), joined_df.name)
    
    best_athletes=joined_df \
        .withColumn("RANK",row_number().over(window_best_ath)) \
        .filter(col("RANK")==1) \
        .select(joined_df.name,joined_df.sport,joined_df.total_points) \
        .orderBy(joined_df.sport)
    
    return best_athletes

def get_top_coaches(athletes_df,coaches_df,medals_df):
    athletes_df=upper_data(athletes_df)
    coaches_df=upper_data(coaches_df)
    medals_df=upper_data(medals_df)
    
    medal_pt=calc_medal_pt(medals_df)
    
    athlete_points=medal_pt.groupBy("id", "YEAR", "event").agg(
        sum("POINTS").alias("TOTAL_POINTS"),
        count(when(col("MEDAL")=="GOLD",True)).alias("GOLD_COUNT"),
        count(when(col("MEDAL")=="SILVER",True)).alias("SILVER_COUNT"),
        count(when(col("MEDAL")=="BRONZE",True)).alias("BRONZE_COUNT"),
    )
    athletes_df=athletes_df.withColumnRenamed("YEAR", "ATHLETE_YEAR")
    athletes_df=athletes_df.withColumnRenamed("country", "ATHLETE_COUNTRY")
    #athletes_df.show()
    joined_ath=athletes_df.join(
        athlete_points,
        (athletes_df.id==athlete_points.id) &
        (athletes_df.ATHLETE_YEAR==athlete_points.YEAR) &
        (athletes_df.event==athlete_points.event)
    )
    coaches_df=coaches_df.withColumnRenamed("country","COACH_COUNTRY")
    #coaches_df.show()
    coaches_year_country=coaches_df.join(
        athletes_df.select("coach_id","ATHLETE_YEAR","ATHLETE_COUNTRY"),
        coaches_df.id==athletes_df.coach_id
    ).withColumn(
        "COACH_YEAR",
        col("ATHLETE_YEAR")
    ).drop("ATHLETE_YEAR").withColumnRenamed("ATHLETE_COUNTRY","country")
    
    #coaches_year_country.show()
    coaches_year_country=coaches_year_country.dropDuplicates(["id","COACH_YEAR","country"])
    
    #if coach_country_check.count()>0:
        #raise ValueError("Found coaches with multiple countries in the same year")
    
    coach_points=joined_ath.join(
        coaches_year_country,
        (joined_ath.coach_id==coaches_year_country.id) & 
        (joined_ath.sport==coaches_year_country.sport) & 
        (joined_ath.YEAR==coaches_year_country.COACH_YEAR)
    ).filter(
        coaches_year_country.country.isin(["CHINA","INDIA","USA"])
    ).groupBy(
        coaches_year_country.id.alias("COACH_ID"),
        coaches_year_country.name.alias("COACH_NAME"),
        coaches_year_country.country,
        coaches_year_country.COACH_YEAR
    ).agg(
        sum("TOTAL_POINTS").alias("TOTAL_POINTS"),
        sum("GOLD_COUNT").alias("GOLD_COUNT"),
        sum("SILVER_COUNT").alias("SILVER_COUNT"),
        sum("BRONZE_COUNT").alias("BRONZE_COUNT")
    )
    #coach_points.show()
    
    country_window=Window.partitionBy("country").orderBy(
        desc("TOTAL_POINTS"),
        desc("GOLD_COUNT"),
        desc("SILVER_COUNT"),
        desc("BRONZE_COUNT"),
        "COACH_NAME"
    )
    
    coach_total_points=coach_points.groupBy(
        "COACH_NAME","country"
    ).agg(
        sum("TOTAL_POINTS").alias("TOTAL_POINTS_SUM"),
        sum("GOLD_COUNT").alias("TOTAL_GOLDS"),
        sum("SILVER_COUNT").alias("TOTAL_SILVERS"),
        sum("BRONZE_COUNT").alias("TOTAL_BRONZES")
    )
   
    # Create a window specification for final ranking
    top_coaches_window=Window.partitionBy("country").orderBy(
        desc("TOTAL_POINTS_SUM"),
        desc("TOTAL_GOLDS"),
        desc("TOTAL_SILVERS"),
        desc("TOTAL_BRONZES"),
        "COACH_NAME"
    )
    top_coaches_per_country=coach_total_points.withColumn(
        "COACH_RANK", 
        row_number().over(top_coaches_window)
    ).filter(col("COACH_RANK")<=5)
    
    ranked_coaches=top_coaches_per_country.withColumn(
        "FINAL_RANK",row_number().over(top_coaches_window)
    ).select(
        "COACH_NAME",
        "country",
        "TOTAL_POINTS_SUM",
        "TOTAL_GOLDS",
        "TOTAL_SILVERS",
        "TOTAL_BRONZES",
        "FINAL_RANK"
    ).orderBy("FINAL_RANK").orderBy(asc("country"))
    
    return ranked_coaches

def main():
    spark = init_spark()
    
    athletes_2012=upper_data(read_csv(spark,sys.argv[1]))
    athletes_2016=upper_data(read_csv(spark,sys.argv[2]))
    athletes_2020=upper_data(read_csv(spark,sys.argv[3]))
    coaches=upper_data(read_csv(spark,sys.argv[4]))
    medals=upper_data(read_csv(spark,sys.argv[5]))
    
    athletes_2012=year_add(athletes_2012,"2012")
    athletes_2016=year_add(athletes_2016,"2016")
    athletes_2020=year_add(athletes_2020,"2020")
    
    #athletes_2012.show()
    #athletes_2016.show()
    #athletes_2020.show()

    ath=athletes_2012.union(athletes_2016).union(athletes_2020)
    ath=filter_years(ath,"YEAR")
    #ath.show()
    coaches=filter_years(coaches,"YEAR")
    #coaches.show()
    medals=filter_years(medals,"YEAR")
    #medals.show()
    
    best_ath=get_best_ath(ath,medals)
    #best_ath.show()
    top_coaches=get_top_coaches(ath,coaches,medals)
    #top_coaches.show()
    
    best_ath_list = ", ".join([f'"{row.name}"' for row in best_ath.collect()])
    top_coaches_list = ", ".join([f'"{row.COACH_NAME}"' for row in top_coaches.collect()])
    
    with open(sys.argv[6], "w") as f:
        f.write(f"([{best_ath_list}],[{top_coaches_list}])")
    
    spark.stop()

if __name__ == "__main__":
    main()
