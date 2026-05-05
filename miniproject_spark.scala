// ============================================================
// INFO 602 - Big Data Analytics  |  Final Mini-Project
// Author: Sanjay Narayanan
// ============================================================

import org.apache.spark.sql.functions._

// STEP 1: Load and clean Overdose data
val overdoseRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("vdh-pud-overdose-ed-visits-by-year-and-geography.csv")

val overdose = overdoseRaw.filter(col("Overdose ED Visit Drug Type") === "All Drug").filter(col("Overdose ED Visit Patient Geography Level") === "Locality").filter(col("Overdose ED Visit Year") === 2023).filter(col("Combined Locality") === "No").select(col("Overdose ED Visit Patient Geography Name").as("locality"), col("Overdose ED Visit Patient FIPS").as("fips"), col("Overdose ED Visit Rate per 10,000 visits").cast("double").as("od_rate")).na.drop()

println("=== Overdose Data Sample ===")
overdose.show(5, truncate = false)
println(s"Overdose record count: ${overdose.count()}")

// STEP 2: Load and clean ACS Poverty data
val acsRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("ACSST5Y2023.S1702-Data.csv")

val acsClean = acsRaw.filter(col("GEO_ID") =!= "Geography").select(regexp_replace(col("NAME"), " County, Virginia|, Virginia", "").as("county"), col("S1702_C02_001E").cast("double").as("pct_poverty"), col("S1702_C01_020E").cast("double").as("count_less_than_hs"), col("S1702_C01_023E").cast("double").as("count_bachelors_plus"), col("S1702_C01_001E").cast("double").as("total_families")).na.drop()

println("=== ACS Data Sample ===")
acsClean.show(5, truncate = false)
println(s"ACS record count: ${acsClean.count()}")

// STEP 3: Join datasets on county name
val joined = overdose.join(acsClean, overdose("locality") === acsClean("county"), "inner").select("locality", "od_rate", "pct_poverty", "count_less_than_hs", "count_bachelors_plus", "total_families")

println("=== Joined Dataset ===")
joined.show(10, truncate = false)
println(s"Joined record count: ${joined.count()}")

// STEP 4: Bucket by poverty level and get average OD rate
val bucketed = joined.withColumn("poverty_bucket", when(col("pct_poverty") < 8, "1_Low (<8%)").when(col("pct_poverty") < 15, "2_Medium (8-15%)").otherwise("3_High (>15%)"))

val avgByPoverty = bucketed.groupBy("poverty_bucket").agg(avg("od_rate").as("avg_od_rate"), count("locality").as("num_counties"), avg("pct_poverty").as("avg_pct_poverty")).orderBy("poverty_bucket")

println("=== Avg OD Rate by Poverty Bucket ===")
avgByPoverty.show(truncate = false)

// STEP 5: Bucket by education level and get average OD rate
val withEduPct = joined.withColumn("pct_bachelors", (col("count_bachelors_plus") / col("total_families") * 100).cast("double"))

val eduBucketed = withEduPct.withColumn("edu_bucket", when(col("pct_bachelors") < 20, "1_Low (<20% bachelors)").when(col("pct_bachelors") < 40, "2_Medium (20-40%)").otherwise("3_High (>40%)"))

val avgByEdu = eduBucketed.groupBy("edu_bucket").agg(avg("od_rate").as("avg_od_rate"), count("locality").as("num_counties"), avg("pct_bachelors").as("avg_pct_bachelors")).orderBy("edu_bucket")

println("=== Avg OD Rate by Education Bucket ===")
avgByEdu.show(truncate = false)

// STEP 6: Top 10 and Bottom 10 counties by OD rate
println("=== Top 10 Counties by OD Rate ===")
joined.orderBy(desc("od_rate")).show(10, truncate = false)

println("=== Bottom 10 Counties by OD Rate ===")
joined.orderBy(asc("od_rate")).show(10, truncate = false)

// STEP 7: Correlation between poverty rate and OD rate
val correlation = joined.stat.corr("pct_poverty", "od_rate")
println(s"=== Correlation (poverty vs OD rate): $correlation ===")
