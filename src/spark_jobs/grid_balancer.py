"""
Grid Balancing MapReduce Job for PowerGrid AI

Pipeline 3: Identify districts at risk of blackout by joining supply and demand.
Join: Demand_RDD + Supply_RDD on District_Key
Logic: Net_Load = Supply - Demand
Filter: Keep districts where Net_Load < 0 (Deficit)
"""
import os
import sys
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.rdd import RDD

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.spark_config import create_spark_session
from config.thresholds import (
    CRITICAL_DEFICIT,
    WARNING_DEFICIT,
    NORMAL_RANGE,
    EXCESS_SURPLUS,
)


class GridBalancer:
    """
    Balances grid supply and demand using MapReduce join operations.

    Demonstrates the JOIN operation in MapReduce - combining two datasets
    by a common key (district_id).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext

    def rdd_join(self, demand_rdd: RDD, supply_rdd: RDD) -> RDD:
        """
        JOIN OPERATION: Combine demand and supply RDDs by district key.

        This is the classic MapReduce join pattern:
        1. Both RDDs have (key, value) structure
        2. Join produces (key, (value1, value2))
        """
        print("\n" + "=" * 60)
        print("GRID BALANCING - MapReduce Join")
        print("=" * 60)

        print("Joining Supply RDD with Demand RDD on district_id...")

        # Join operation: (district_id, demand) JOIN (district_id, supply)
        # Result: (district_id, (demand, supply))
        joined_rdd = demand_rdd.join(supply_rdd)

        print(f"Joined {joined_rdd.count()} district records")
        return joined_rdd

    def calculate_net_load(self, joined_rdd: RDD) -> RDD:
        """
        MAP: Calculate net load (supply - demand) for each district.

        Map: (district_id, (demand, supply)) -> (district_id, net_load, status)
        """
        print("Calculating net load: Supply - Demand")

        def calculate_balance(record):
            district_id, (demand, supply) = record

            # Convert MWh demand to MW for comparison (assuming hourly readings)
            # For simplicity, we aggregate totals
            demand_mw = demand / 1000  # Convert kWh to MWh, rough approximation
            supply_mw = supply

            net_load = supply_mw - demand_mw

            # Calculate ratio for status
            if supply_mw > 0:
                ratio = net_load / supply_mw
            else:
                ratio = -1.0

            # Determine status
            if ratio < CRITICAL_DEFICIT:
                status = "CRITICAL"
            elif ratio < WARNING_DEFICIT:
                status = "WARNING"
            elif NORMAL_RANGE[0] <= ratio <= NORMAL_RANGE[1]:
                status = "NORMAL"
            elif ratio > EXCESS_SURPLUS:
                status = "EXCESS"
            else:
                status = "OK"

            return (district_id, demand_mw, supply_mw, net_load, ratio, status)

        balanced_rdd = joined_rdd.map(calculate_balance)
        return balanced_rdd

    def filter_deficit_districts(self, balanced_rdd: RDD) -> RDD:
        """
        FILTER: Keep only districts with negative net load (deficit).
        """
        print("Filtering deficit districts (Net_Load < 0)...")

        deficit_rdd = balanced_rdd.filter(lambda x: x[3] < 0)  # net_load is index 3

        deficit_count = deficit_rdd.count()
        print(f"Found {deficit_count} districts with supply deficit")
        return deficit_rdd

    def filter_critical_districts(self, balanced_rdd: RDD) -> RDD:
        """
        FILTER: Keep only critical status districts.
        """
        print("Filtering CRITICAL status districts...")

        critical_rdd = balanced_rdd.filter(lambda x: x[5] == "CRITICAL")  # status is index 5

        return critical_rdd

    def balance_grid(
        self,
        demand_df: DataFrame,
        supply_df: DataFrame,
    ) -> DataFrame:
        """
        Execute the full grid balancing pipeline.

        1. Convert DataFrames to RDDs with (district_id, value)
        2. Join on district_id
        3. Calculate net load
        4. Filter and classify
        """
        print("\nConverting DataFrames to RDDs...")

        # Prepare demand RDD: (district_id, total_demand)
        demand_rdd = demand_df.rdd.map(
            lambda row: (row.district_id, row.total_demand_kwh)
        )

        # Prepare supply RDD: (district_id, total_supply)
        supply_rdd = supply_df.rdd.map(
            lambda row: (row.district_id, row.total_supply_mw)
        )

        # JOIN
        joined_rdd = self.rdd_join(demand_rdd, supply_rdd)

        # CALCULATE NET LOAD
        balanced_rdd = self.calculate_net_load(joined_rdd)

        # Convert to DataFrame
        result_df = balanced_rdd.toDF([
            "district_id",
            "demand_mwh",
            "supply_mw",
            "net_load_mw",
            "balance_ratio",
            "status"
        ])

        result_df = result_df.orderBy("net_load_mw")
        return result_df

    def balance_grid_hourly(
        self,
        hourly_demand_df: DataFrame,
        hourly_supply_df: DataFrame,
    ) -> DataFrame:
        """
        Balance grid at hourly granularity.

        Uses composite key (district_id, hour) for join.
        """
        print("\n" + "=" * 60)
        print("HOURLY GRID BALANCING - MapReduce Join")
        print("=" * 60)

        # Prepare RDDs with composite key
        demand_rdd = hourly_demand_df.rdd.map(
            lambda row: ((row.district_id, row.hour), row.total_demand_kwh)
        )

        supply_rdd = hourly_supply_df.rdd.map(
            lambda row: ((row.district_id, row.hour), row.total_supply_mw)
        )

        # JOIN on composite key
        print("Joining on (district_id, hour) composite key...")
        joined_rdd = demand_rdd.join(supply_rdd)

        # Calculate balance
        def calculate_hourly_balance(record):
            (district_id, hour), (demand, supply) = record
            demand_mw = demand / 1000
            net_load = supply - demand_mw

            if supply > 0:
                ratio = net_load / supply
            else:
                ratio = -1.0

            if ratio < CRITICAL_DEFICIT:
                status = "CRITICAL"
            elif ratio < WARNING_DEFICIT:
                status = "WARNING"
            else:
                status = "OK"

            return (district_id, hour, demand_mw, supply, net_load, ratio, status)

        balanced_rdd = joined_rdd.map(calculate_hourly_balance)

        result_df = balanced_rdd.toDF([
            "district_id",
            "hour",
            "demand_mwh",
            "supply_mw",
            "net_load_mw",
            "balance_ratio",
            "status"
        ])

        return result_df.orderBy("district_id", "hour")

    def get_blackout_risk_summary(self, balanced_df: DataFrame) -> Dict:
        """
        Generate blackout risk summary statistics.
        """
        print("\nGenerating blackout risk summary...")

        # Count by status
        status_counts = balanced_df.groupBy("status").count().collect()
        status_dict = {row.status: row["count"] for row in status_counts}

        # Get critical districts
        critical_df = balanced_df.filter(F.col("status") == "CRITICAL")
        critical_districts = [row.district_id for row in critical_df.collect()]

        # Calculate total deficit
        deficit_df = balanced_df.filter(F.col("net_load_mw") < 0)
        total_deficit = deficit_df.agg(F.sum("net_load_mw")).collect()[0][0] or 0

        return {
            "status_counts": status_dict,
            "critical_districts": critical_districts,
            "total_deficit_mw": total_deficit,
            "districts_at_risk": len(critical_districts),
        }

    def save_results(self, df: DataFrame, output_path: str, name: str):
        """Save results to CSV."""
        print(f"Saving {name} to {output_path}...")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


def run_grid_balancing(
    spark: SparkSession,
    demand_results: Dict,
    supply_results: Dict,
    output_dir: str,
) -> Dict:
    """
    Run the complete grid balancing pipeline.
    """
    balancer = GridBalancer(spark)

    # Get total aggregations
    total_demand = demand_results["total_demand"]
    total_supply = supply_results["total_supply"]

    # Balance grid
    balanced_df = balancer.balance_grid(total_demand, total_supply)

    # Get hourly balance if available
    hourly_balance = None
    if "hourly_demand" in demand_results and "hourly_supply" in supply_results:
        hourly_balance = balancer.balance_grid_hourly(
            demand_results["hourly_demand"],
            supply_results["hourly_supply"]
        )

    # Get risk summary
    risk_summary = balancer.get_blackout_risk_summary(balanced_df)

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    balancer.save_results(balanced_df, os.path.join(output_dir, "grid_balance"), "grid balance")
    if hourly_balance is not None:
        balancer.save_results(hourly_balance, os.path.join(output_dir, "hourly_balance"), "hourly balance")

    # Print summary
    print("\n" + "=" * 60)
    print("GRID BALANCING RESULTS")
    print("=" * 60)

    print("\nGrid Balance by District:")
    balanced_df.show(20, truncate=False)

    print("\nBlackout Risk Summary:")
    print(f"  Status Distribution: {risk_summary['status_counts']}")
    print(f"  Critical Districts: {risk_summary['critical_districts']}")
    print(f"  Total Deficit: {risk_summary['total_deficit_mw']:.2f} MW")
    print(f"  Districts at Risk: {risk_summary['districts_at_risk']}")

    return {
        "balanced": balanced_df,
        "hourly_balance": hourly_balance,
        "risk_summary": risk_summary,
    }


def main():
    """Main function for standalone execution."""
    # This requires demand and supply results - run from main.py
    print("Grid Balancer should be run from main.py with demand and supply results.")
    print("Run: python src/main.py")


if __name__ == "__main__":
    main()
