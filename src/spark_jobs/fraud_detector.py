"""
Fraud Detection MapReduce Job for PowerGrid AI

Pipeline 4: Detect energy theft using statistical Z-score analysis.
Pass 1 (Stats): Calculate Mean & StdDev of usage per district
Pass 2 (Detection): Calculate Z-Score for every meter
Filter: Flag meters where Z > 3.0 (Statistical Outlier)
"""
import os
import sys
import math
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.rdd import RDD

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.spark_config import create_spark_session
from config.thresholds import (
    FRAUD_ZSCORE_CRITICAL,
    FRAUD_ZSCORE_WARNING,
    FRAUD_ZSCORE_MONITOR,
    MIN_SAMPLES_FOR_STATS,
)
from config.grid_parameters import BASE_ELECTRICITY_PRICE


class FraudDetector:
    """
    Detects energy fraud using statistical MapReduce analysis.

    Uses a two-pass MapReduce approach:
    1. First pass: Calculate statistics (mean, stddev) per district
    2. Second pass: Calculate Z-scores and flag anomalies
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext

    def load_meter_data(self, file_path: str) -> DataFrame:
        """Load smart meter readings."""
        print(f"Loading meter data from {file_path}...")

        df = self.spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )

        print(f"Loaded {df.count():,} records")
        return df

    def pass1_calculate_statistics(self, df: DataFrame) -> RDD:
        """
        PASS 1: Calculate mean and standard deviation per district.

        Map: (meter_reading) -> (district_id, (kwh, kwh^2, 1))
        Reduce: (district_id, [(kwh, kwh^2, count)]) -> (district_id, (sum, sum_sq, n))
        Post-process: Calculate mean and stddev from aggregates
        """
        print("\n" + "=" * 60)
        print("FRAUD DETECTION - Pass 1: Calculate Statistics")
        print("=" * 60)

        # First, aggregate per meter to get total consumption per meter
        print("Aggregating consumption per meter...")
        meter_totals = df.rdd.map(
            lambda row: (row.meter_id, (row.district_id, row.kwh))
        ).reduceByKey(
            lambda a, b: (a[0], a[1] + b[1])  # Keep district, sum kwh
        ).map(
            lambda x: (x[0], x[1][0], x[1][1])  # (meter_id, district_id, total_kwh)
        )

        # MAP: Transform to (district, (kwh, kwh^2, 1)) for variance calculation
        print("MAP: (meter_total) -> (district_id, (kwh, kwh^2, 1))")
        mapped_rdd = meter_totals.map(
            lambda x: (x[1], (x[2], x[2] ** 2, 1))  # (district, (kwh, kwh^2, count))
        )

        # REDUCE: Sum all values per district
        print("REDUCE: Aggregating statistics per district...")
        reduced_rdd = mapped_rdd.reduceByKey(
            lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
        )

        # Calculate mean and stddev from aggregates
        # mean = sum / n
        # variance = (sum_sq / n) - (mean^2)
        # stddev = sqrt(variance)
        def calc_stats(record):
            district_id, (sum_kwh, sum_sq, count) = record

            if count < MIN_SAMPLES_FOR_STATS:
                return (district_id, 0, 0, count, False)  # Not enough samples

            mean = sum_kwh / count
            variance = (sum_sq / count) - (mean ** 2)
            stddev = math.sqrt(max(0, variance))  # Ensure non-negative

            return (district_id, mean, stddev, count, True)

        stats_rdd = reduced_rdd.map(calc_stats)

        print("Statistics calculated for all districts")
        return stats_rdd, meter_totals

    def pass2_calculate_zscores(self, meter_totals: RDD, stats_rdd: RDD) -> RDD:
        """
        PASS 2: Calculate Z-scores for each meter.

        Join meter data with district statistics, then calculate Z-score.
        Z = (value - mean) / stddev
        """
        print("\n" + "=" * 60)
        print("FRAUD DETECTION - Pass 2: Calculate Z-Scores")
        print("=" * 60)

        # Prepare stats for join: (district_id, (mean, stddev))
        stats_for_join = stats_rdd.filter(lambda x: x[4]).map(  # Filter valid stats
            lambda x: (x[0], (x[1], x[2]))  # (district_id, (mean, stddev))
        )

        # Prepare meter data for join: (district_id, (meter_id, kwh))
        meters_for_join = meter_totals.map(
            lambda x: (x[1], (x[0], x[2]))  # (district_id, (meter_id, total_kwh))
        )

        # JOIN: Combine meters with their district statistics
        print("Joining meter data with district statistics...")
        joined_rdd = meters_for_join.join(stats_for_join)
        # Result: (district_id, ((meter_id, kwh), (mean, stddev)))

        # Calculate Z-score
        def calc_zscore(record):
            district_id, ((meter_id, kwh), (mean, stddev)) = record

            if stddev > 0:
                zscore = (kwh - mean) / stddev
            else:
                zscore = 0

            # Determine fraud status
            if abs(zscore) >= FRAUD_ZSCORE_CRITICAL:
                status = "FRAUD_LIKELY"
            elif abs(zscore) >= FRAUD_ZSCORE_WARNING:
                status = "SUSPICIOUS"
            elif abs(zscore) >= FRAUD_ZSCORE_MONITOR:
                status = "MONITOR"
            else:
                status = "NORMAL"

            # Flag negative Z-scores (under-reporting) as more suspicious
            # This indicates potential meter tampering to report less usage
            if zscore < -FRAUD_ZSCORE_CRITICAL:
                fraud_type = "UNDER_REPORTING"
            elif zscore > FRAUD_ZSCORE_CRITICAL:
                fraud_type = "OVER_REPORTING"
            else:
                fraud_type = "NONE"

            return (meter_id, district_id, kwh, mean, stddev, zscore, status, fraud_type)

        print("Calculating Z-scores for all meters...")
        zscore_rdd = joined_rdd.map(calc_zscore)

        return zscore_rdd

    def filter_fraudulent_meters(self, zscore_rdd: RDD) -> RDD:
        """
        FILTER: Keep only meters flagged as fraudulent.
        """
        print("\nFiltering fraudulent meters (Z > 3.0 or Z < -3.0)...")

        fraud_rdd = zscore_rdd.filter(
            lambda x: x[6] in ["FRAUD_LIKELY", "SUSPICIOUS"]  # status
        )

        count = fraud_rdd.count()
        print(f"Found {count} potentially fraudulent meters")
        return fraud_rdd

    def detect_fraud(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, Dict]:
        """
        Execute the full fraud detection pipeline.
        """
        # PASS 1: Calculate statistics
        stats_rdd, meter_totals = self.pass1_calculate_statistics(df)

        # PASS 2: Calculate Z-scores
        zscore_rdd = self.pass2_calculate_zscores(meter_totals, stats_rdd)

        # Convert to DataFrames
        stats_df = stats_rdd.toDF([
            "district_id", "mean_kwh", "stddev_kwh", "meter_count", "valid_stats"
        ])

        zscore_df = zscore_rdd.toDF([
            "meter_id", "district_id", "total_kwh", "district_mean",
            "district_stddev", "zscore", "status", "fraud_type"
        ])

        # Get fraud summary
        fraud_df = zscore_df.filter(F.col("status").isin(["FRAUD_LIKELY", "SUSPICIOUS"]))

        # Calculate financial impact
        fraud_summary = self._calculate_fraud_impact(fraud_df, zscore_df)

        return stats_df, zscore_df, fraud_summary

    def _calculate_fraud_impact(self, fraud_df: DataFrame, all_meters_df: DataFrame) -> Dict:
        """Calculate the financial impact of detected fraud."""
        print("\nCalculating fraud impact...")

        # Count by status
        status_counts = all_meters_df.groupBy("status").count().collect()
        status_dict = {row.status: row["count"] for row in status_counts}

        # Get fraud meters
        fraud_meters = fraud_df.collect()

        # Calculate estimated stolen energy
        # For under-reporters: estimated_actual = mean * (1 + |zscore|/3)
        # Stolen = estimated_actual - reported
        total_stolen_kwh = 0
        for row in fraud_meters:
            if row.fraud_type == "UNDER_REPORTING":
                # Estimate actual usage based on district mean and Z-score
                estimated_actual = row.district_mean * (1 + abs(row.zscore) / 3)
                stolen = estimated_actual - row.total_kwh
                total_stolen_kwh += max(0, stolen)

        # Financial impact
        revenue_loss = total_stolen_kwh * BASE_ELECTRICITY_PRICE

        return {
            "status_distribution": status_dict,
            "total_fraud_cases": fraud_df.count(),
            "total_meters_analyzed": all_meters_df.count(),
            "fraud_rate_pct": (fraud_df.count() / all_meters_df.count()) * 100 if all_meters_df.count() > 0 else 0,
            "estimated_stolen_kwh": total_stolen_kwh,
            "estimated_revenue_loss": revenue_loss,
        }

    def save_results(self, df: DataFrame, output_path: str, name: str):
        """Save results to CSV."""
        print(f"Saving {name} to {output_path}...")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


def run_fraud_detection(
    spark: SparkSession,
    input_path: str,
    output_dir: str,
) -> Dict:
    """
    Run the complete fraud detection pipeline.
    """
    detector = FraudDetector(spark)

    # Load data
    meter_df = detector.load_meter_data(input_path)

    # Detect fraud
    stats_df, zscore_df, fraud_summary = detector.detect_fraud(meter_df)

    # Get fraud cases
    fraud_df = zscore_df.filter(F.col("status").isin(["FRAUD_LIKELY", "SUSPICIOUS"]))

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    detector.save_results(stats_df, os.path.join(output_dir, "district_stats"), "district statistics")
    detector.save_results(zscore_df, os.path.join(output_dir, "meter_zscores"), "meter Z-scores")
    detector.save_results(fraud_df, os.path.join(output_dir, "fraud_cases"), "fraud cases")

    # Print summary
    print("\n" + "=" * 60)
    print("FRAUD DETECTION RESULTS")
    print("=" * 60)

    print("\nDistrict Statistics:")
    stats_df.show(10, truncate=False)

    print("\nTop Suspicious Meters (by Z-score magnitude):")
    zscore_df.orderBy(F.abs(F.col("zscore")).desc()).show(20, truncate=False)

    print("\nFraud Summary:")
    print(f"  Total Meters Analyzed: {fraud_summary['total_meters_analyzed']:,}")
    print(f"  Fraud Cases Detected: {fraud_summary['total_fraud_cases']}")
    print(f"  Fraud Rate: {fraud_summary['fraud_rate_pct']:.2f}%")
    print(f"  Status Distribution: {fraud_summary['status_distribution']}")
    print(f"  Estimated Stolen Energy: {fraud_summary['estimated_stolen_kwh']:,.2f} kWh")
    print(f"  Estimated Revenue Loss: ${fraud_summary['estimated_revenue_loss']:,.2f}")

    return {
        "stats": stats_df,
        "zscores": zscore_df,
        "fraud_cases": fraud_df,
        "summary": fraud_summary,
    }


def main():
    """Main function for standalone execution."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_path = os.path.join(project_root, "data", "raw", "meter_readings.csv")
    output_dir = os.path.join(project_root, "data", "processed", "fraud")

    print("=" * 60)
    print("PowerGrid AI - Fraud Detector")
    print("=" * 60)

    spark = create_spark_session("FraudDetector")

    try:
        results = run_fraud_detection(spark, input_path, output_dir)
        print("\nFraud detection complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
