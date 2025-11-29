"""
Demand Aggregation MapReduce Job for PowerGrid AI

Pipeline 1: Calculate total energy consumption per district using MapReduce.
Map: (Meter_Log) -> (District, kWh)
Reduce: (District, [kWh_List]) -> (District, Total_Demand)
"""
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.rdd import RDD

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.spark_config import create_spark_session


class DemandAggregator:
    """
    Aggregates energy demand per district using MapReduce paradigm.

    This class demonstrates the classic MapReduce pattern:
    1. Map: Extract (key, value) pairs from raw data
    2. Reduce: Aggregate values by key
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext

    def load_meter_data(self, file_path: str) -> DataFrame:
        """Load smart meter readings from CSV."""
        print(f"Loading meter data from {file_path}...")

        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("meter_id", StringType(), True),
            StructField("district_id", StringType(), True),
            StructField("meter_type", StringType(), True),
            StructField("kwh", DoubleType(), True),
            StructField("voltage", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
        ])

        df = self.spark.read.csv(
            file_path,
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )

        count = df.count()
        print(f"Loaded {count:,} meter readings")
        return df

    def map_phase(self, df: DataFrame) -> RDD:
        """
        MAP PHASE: Transform each meter reading into (district_id, kwh) pair.

        This is the classic Map operation where we extract the key (district)
        and value (consumption) from each record.
        """
        print("Executing MAP phase: (meter_log) -> (district_id, kwh)")

        # Convert DataFrame to RDD and map to (key, value) pairs
        rdd = df.rdd.map(lambda row: (row.district_id, row.kwh))

        return rdd

    def reduce_phase(self, mapped_rdd: RDD) -> RDD:
        """
        REDUCE PHASE: Sum all kWh values for each district.

        This is the classic Reduce operation where we aggregate
        all values that share the same key.
        """
        print("Executing REDUCE phase: (district_id, [kwh_list]) -> (district_id, total_demand)")

        # reduceByKey is the classic MapReduce reduce operation
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        return reduced_rdd

    def aggregate_demand_mapreduce(self, df: DataFrame) -> DataFrame:
        """
        Execute the full MapReduce pipeline using RDD operations.

        This demonstrates the pure MapReduce paradigm.
        """
        print("\n" + "=" * 60)
        print("DEMAND AGGREGATION - MapReduce Pipeline")
        print("=" * 60)

        # MAP
        mapped_rdd = self.map_phase(df)

        # REDUCE
        reduced_rdd = self.reduce_phase(mapped_rdd)

        # Convert back to DataFrame for easier handling
        result_df = reduced_rdd.toDF(["district_id", "total_demand_kwh"])

        # Sort by district
        result_df = result_df.orderBy("district_id")

        print(f"Aggregated demand for {result_df.count()} districts")
        return result_df

    def aggregate_demand_by_hour(self, df: DataFrame) -> DataFrame:
        """
        Aggregate demand by district and hour using MapReduce.

        Map: (meter_log) -> ((district_id, hour), kwh)
        Reduce: ((district_id, hour), [kwh_list]) -> ((district_id, hour), total)
        """
        print("\n" + "=" * 60)
        print("HOURLY DEMAND AGGREGATION - MapReduce Pipeline")
        print("=" * 60)

        # MAP: Create composite key (district, hour)
        print("MAP: (meter_log) -> ((district_id, hour), kwh)")
        mapped_rdd = df.rdd.map(
            lambda row: ((row.district_id, row.timestamp.hour), row.kwh)
        )

        # REDUCE: Sum by composite key
        print("REDUCE: ((district_id, hour), [kwh_list]) -> ((district_id, hour), total)")
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        # Flatten the key for DataFrame conversion
        flattened_rdd = reduced_rdd.map(
            lambda x: (x[0][0], x[0][1], x[1])
        )

        result_df = flattened_rdd.toDF(["district_id", "hour", "total_demand_kwh"])
        result_df = result_df.orderBy("district_id", "hour")

        return result_df

    def aggregate_demand_by_type(self, df: DataFrame) -> DataFrame:
        """
        Aggregate demand by meter type using MapReduce.

        Map: (meter_log) -> (meter_type, kwh)
        Reduce: (meter_type, [kwh_list]) -> (meter_type, total)
        """
        print("\n" + "=" * 60)
        print("DEMAND BY TYPE - MapReduce Pipeline")
        print("=" * 60)

        # MAP
        print("MAP: (meter_log) -> (meter_type, kwh)")
        mapped_rdd = df.rdd.map(lambda row: (row.meter_type, row.kwh))

        # REDUCE
        print("REDUCE: (meter_type, [kwh_list]) -> (meter_type, total)")
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        result_df = reduced_rdd.toDF(["meter_type", "total_demand_kwh"])
        return result_df

    def get_peak_demand_districts(self, df: DataFrame, top_n: int = 5) -> DataFrame:
        """
        Find districts with highest peak demand.

        Uses MapReduce with additional sorting/filtering.
        """
        print(f"\nFinding top {top_n} peak demand districts...")

        # First aggregate by district
        demand_df = self.aggregate_demand_mapreduce(df)

        # Get top N by demand
        top_districts = demand_df.orderBy(
            F.col("total_demand_kwh").desc()
        ).limit(top_n)

        return top_districts

    def save_results(self, df: DataFrame, output_path: str, name: str):
        """Save aggregation results to CSV."""
        print(f"Saving {name} to {output_path}...")

        # Coalesce to single file for readability
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

        print(f"Saved {df.count()} records")


def run_demand_aggregation(spark: SparkSession, input_path: str, output_dir: str) -> Dict:
    """
    Run the complete demand aggregation pipeline.

    Returns a dictionary of results for downstream processing.
    """
    aggregator = DemandAggregator(spark)

    # Load data
    meter_df = aggregator.load_meter_data(input_path)

    # Run aggregations
    total_demand = aggregator.aggregate_demand_mapreduce(meter_df)
    hourly_demand = aggregator.aggregate_demand_by_hour(meter_df)
    type_demand = aggregator.aggregate_demand_by_type(meter_df)
    peak_districts = aggregator.get_peak_demand_districts(meter_df)

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    aggregator.save_results(total_demand, os.path.join(output_dir, "total_demand"), "total demand")
    aggregator.save_results(hourly_demand, os.path.join(output_dir, "hourly_demand"), "hourly demand")
    aggregator.save_results(type_demand, os.path.join(output_dir, "type_demand"), "demand by type")

    # Print summary
    print("\n" + "=" * 60)
    print("DEMAND AGGREGATION RESULTS")
    print("=" * 60)

    print("\nTotal Demand by District:")
    total_demand.show(10, truncate=False)

    print("\nDemand by Meter Type:")
    type_demand.show(truncate=False)

    print(f"\nTop 5 Peak Demand Districts:")
    peak_districts.show(truncate=False)

    return {
        "total_demand": total_demand,
        "hourly_demand": hourly_demand,
        "type_demand": type_demand,
        "peak_districts": peak_districts,
    }


def main():
    """Main function for standalone execution."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_path = os.path.join(project_root, "data", "raw", "meter_readings.csv")
    output_dir = os.path.join(project_root, "data", "processed", "demand")

    print("=" * 60)
    print("PowerGrid AI - Demand Aggregator")
    print("=" * 60)

    spark = create_spark_session("DemandAggregator")

    try:
        results = run_demand_aggregation(spark, input_path, output_dir)
        print("\nDemand aggregation complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
