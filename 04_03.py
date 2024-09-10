##import required libraries
import pyspark

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "C:/Users/Nikesh/Downloads/postgresql-42.7.4.jar") \
   .getOrCreate()


##read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
   .option("dbtable", "movies") \
   .option("user", "postgres") \
   .option("password", "helloWorld") \
   .option("driver", "org.postgresql.Driver") \
   .load()
   
##add code below
user_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
   .option("dbtable", "users") \
   .option("user", "postgres") \
   .option("password", "helloWorld") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the users dataframe
print(user_df.show())

avg_rating = user_df.groupBy("movie_id").mean("rating")

df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)

print(movies_df.show())
print(user_df.show())
print(df.show())
