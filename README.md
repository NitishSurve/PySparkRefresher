# PySparkRefresher
Pyspark refresher using Databricks, focusing on data cleaning, null handling, salary transformation, aggregation, and exploratory analysis. Implemented transformations with withColumn, groupBy, and SQL queries to derive insights such as salary trends by gender, city, and job roles. The project focused on cleaning, transforming, and analyzing structured data using PySpark DataFrame APIs and SparkSQL.

Key tasks included:

🧹 Data Cleaning & Preparation: Handling missing values in columns such as city, salary, latitude, and longitude using conditional transformations (when, otherwise) and statistical replacements like mean and median.

🔄 Feature Engineering: Creating new fields such as cleaned salary values, derived indicators (e.g., identifying records related to specific countries), and normalized city information.

📊 Data Analysis & Aggregations: Using groupBy, avg, sum, min, max, and countDistinct to compute metrics such as average salary by gender, salary differences between males and females across job roles, and average salary by city.

⚡ PySpark Transformations: Applying functions like withColumn, filter, substr, cast, and round to manipulate and standardize data efficiently.

🧠 Insights Generation: Exploring salary patterns across genders, job titles, and locations to identify trends and disparities.

🗄️ Spark SQL Integration: Creating temporary views and running SQL queries directly on Spark DataFrames for additional analysis.

This project strengthened my understanding of distributed data processing, PySpark transformations, data wrangling, and analytical workflows in a big-data environment.


