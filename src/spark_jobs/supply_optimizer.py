"""
Supply Optimization MapReduce Job for PowerGrid AI

Pipeline 2: Calculate available power generation adjusted for weather.
Map: (Plant_Log, Weather) -> (District, Adjusted_MW)
Reduce: (District, [MW_List]) -> (District, Total_Supply)
"""
import os
import sys
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.rdd import RDD

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.spark_config import create_spark_session
from config.grid_parameters import EMISSION_FACTORS, OPERATING_COSTS


class SupplyOptimizer:
    """
    Optimizes and aggregates power supply per district using MapReduce.

    Demonstrates MapReduce with data enrichment (weather adjustment).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext

    def load_plant_data(self, file_path: str) -> DataFrame:
        """Load power plant output data."""
        print(f"Loading plant output data from {file_path}...")

        df = self.spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )

        count = df.count()
        print(f"Loaded {count:,} plant output records")
        return df

    def load_weather_data(self, file_path: str) -> DataFrame:
        """Load weather data."""
        print(f"Loading weather data from {file_path}...")

        df = self.spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )

        print(f"Loaded {df.count()} weather records")
        return df

    def enrich_with_weather(self, plant_df: DataFrame, weather_df: DataFrame) -> DataFrame:
        """
        Join plant output with weather data for supply calculations.

        This is a pre-processing step before MapReduce.
        """
        print("Enriching plant data with weather factors...")

        # Join on timestamp
        enriched_df = plant_df.join(
            weather_df.select("timestamp", "cloud_factor", "wind_factor", "precipitation_factor"),
            on="timestamp",
            how="left"
        )

        # Fill any missing weather data with defaults
        enriched_df = enriched_df.fillna({
            "cloud_factor": 0.7,
            "wind_factor": 0.5,
            "precipitation_factor": 1.0
        })

        return enriched_df

    def map_phase(self, df: DataFrame) -> RDD:
        """
        MAP PHASE: Transform plant output to (district_id, adjusted_mw).

        The weather adjustment is already applied during data generation,
        so we extract the actual output_mw which is weather-adjusted.
        """
        print("Executing MAP phase: (plant_output) -> (district_id, output_mw)")

        # Map to (district, output_mw) pairs
        rdd = df.rdd.map(lambda row: (row.district_id, row.output_mw))

        return rdd

    def reduce_phase(self, mapped_rdd: RDD) -> RDD:
        """
        REDUCE PHASE: Sum all MW values for each district.
        """
        print("Executing REDUCE phase: (district_id, [mw_list]) -> (district_id, total_supply)")

        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        return reduced_rdd

    def aggregate_supply_mapreduce(self, df: DataFrame) -> DataFrame:
        """
        Execute the full MapReduce pipeline for supply aggregation.
        """
        print("\n" + "=" * 60)
        print("SUPPLY OPTIMIZATION - MapReduce Pipeline")
        print("=" * 60)

        # MAP
        mapped_rdd = self.map_phase(df)

        # REDUCE
        reduced_rdd = self.reduce_phase(mapped_rdd)

        # Convert to DataFrame
        result_df = reduced_rdd.toDF(["district_id", "total_supply_mw"])
        result_df = result_df.orderBy("district_id")

        print(f"Aggregated supply for {result_df.count()} districts")
        return result_df

    def aggregate_supply_by_type(self, df: DataFrame) -> DataFrame:
        """
        Aggregate supply by plant type using MapReduce.

        Map: (plant_output) -> (plant_type, output_mw)
        Reduce: (plant_type, [mw_list]) -> (plant_type, total)
        """
        print("\n" + "=" * 60)
        print("SUPPLY BY TYPE - MapReduce Pipeline")
        print("=" * 60)

        # MAP
        print("MAP: (plant_output) -> (plant_type, output_mw)")
        mapped_rdd = df.rdd.map(lambda row: (row.plant_type, row.output_mw))

        # REDUCE
        print("REDUCE: (plant_type, [mw_list]) -> (plant_type, total)")
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        result_df = reduced_rdd.toDF(["plant_type", "total_supply_mw"])
        return result_df

    def aggregate_supply_by_hour(self, df: DataFrame) -> DataFrame:
        """
        Aggregate supply by district and hour.

        Map: (plant_output) -> ((district_id, hour), output_mw)
        Reduce: ((district_id, hour), [mw_list]) -> ((district_id, hour), total)
        """
        print("\n" + "=" * 60)
        print("HOURLY SUPPLY - MapReduce Pipeline")
        print("=" * 60)

        # MAP with composite key
        print("MAP: (plant_output) -> ((district_id, hour), output_mw)")
        mapped_rdd = df.rdd.map(
            lambda row: ((row.district_id, row.timestamp.hour), row.output_mw)
        )

        # REDUCE
        print("REDUCE: ((district_id, hour), [mw_list]) -> ((district_id, hour), total)")
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        # Flatten
        flattened_rdd = reduced_rdd.map(lambda x: (x[0][0], x[0][1], x[1]))

        result_df = flattened_rdd.toDF(["district_id", "hour", "total_supply_mw"])
        result_df = result_df.orderBy("district_id", "hour")

        return result_df

    def calculate_capacity_utilization(self, df: DataFrame) -> DataFrame:
        """
        Calculate capacity utilization per plant type.

        Map: (plant) -> (plant_type, (output_mw, capacity_mw))
        Reduce: Sum both and calculate ratio
        """
        print("\nCalculating capacity utilization...")

        # MAP: Extract output and capacity
        mapped_rdd = df.rdd.map(
            lambda row: (row.plant_type, (row.output_mw, row.capacity_mw))
        )

        # REDUCE: Sum both values
        reduced_rdd = mapped_rdd.reduceByKey(
            lambda a, b: (a[0] + b[0], a[1] + b[1])
        )

        # Calculate utilization ratio
        utilization_rdd = reduced_rdd.map(
            lambda x: (x[0], x[1][0], x[1][1], x[1][0] / x[1][1] if x[1][1] > 0 else 0)
        )

        result_df = utilization_rdd.toDF([
            "plant_type", "total_output_mw", "total_capacity_mw", "utilization_rate"
        ])

        return result_df

    def calculate_operating_costs(self, df: DataFrame) -> DataFrame:
        """
        Calculate total operating costs by plant type using MapReduce.
        """
        print("\nCalculating operating costs...")

        # MAP: (plant_type, operating_cost)
        mapped_rdd = df.rdd.map(
            lambda row: (row.plant_type, row.operating_cost)
        )

        # REDUCE: Sum costs
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        result_df = reduced_rdd.toDF(["plant_type", "total_operating_cost"])
        return result_df

    def save_results(self, df: DataFrame, output_path: str, name: str):
        """Save results to CSV."""
        print(f"Saving {name} to {output_path}...")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        print(f"Saved {df.count()} records")


def run_supply_optimization(
    spark: SparkSession,
    plant_path: str,
    weather_path: str,
    output_dir: str
) -> Dict:
    """
    Run the complete supply optimization pipeline.
    """
    optimizer = SupplyOptimizer(spark)

    # Load data
    plant_df = optimizer.load_plant_data(plant_path)
    weather_df = optimizer.load_weather_data(weather_path)

    # Enrich with weather
    enriched_df = optimizer.enrich_with_weather(plant_df, weather_df)

    # Run aggregations
    total_supply = optimizer.aggregate_supply_mapreduce(enriched_df)
    type_supply = optimizer.aggregate_supply_by_type(enriched_df)
    hourly_supply = optimizer.aggregate_supply_by_hour(enriched_df)
    utilization = optimizer.calculate_capacity_utilization(enriched_df)
    costs = optimizer.calculate_operating_costs(enriched_df)

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    optimizer.save_results(total_supply, os.path.join(output_dir, "total_supply"), "total supply")
    optimizer.save_results(type_supply, os.path.join(output_dir, "type_supply"), "supply by type")
    optimizer.save_results(hourly_supply, os.path.join(output_dir, "hourly_supply"), "hourly supply")
    optimizer.save_results(utilization, os.path.join(output_dir, "utilization"), "capacity utilization")
    optimizer.save_results(costs, os.path.join(output_dir, "costs"), "operating costs")

    # Print summary
    print("\n" + "=" * 60)
    print("SUPPLY OPTIMIZATION RESULTS")
    print("=" * 60)

    print("\nTotal Supply by District:")
    total_supply.show(10, truncate=False)

    print("\nSupply by Plant Type:")
    type_supply.show(truncate=False)

    print("\nCapacity Utilization:")
    utilization.show(truncate=False)

    print("\nOperating Costs by Plant Type:")
    costs.show(truncate=False)

    return {
        "total_supply": total_supply,
        "type_supply": type_supply,
        "hourly_supply": hourly_supply,
        "utilization": utilization,
        "costs": costs,
    }


def main():
    """Main function for standalone execution."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    plant_path = os.path.join(project_root, "data", "raw", "plant_output.csv")
    weather_path = os.path.join(project_root, "data", "raw", "weather.csv")
    output_dir = os.path.join(project_root, "data", "processed", "supply")

    print("=" * 60)
    print("PowerGrid AI - Supply Optimizer")
    print("=" * 60)

    spark = create_spark_session("SupplyOptimizer")

    try:
        results = run_supply_optimization(spark, plant_path, weather_path, output_dir)
        print("\nSupply optimization complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
