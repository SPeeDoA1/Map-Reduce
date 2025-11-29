"""
Carbon Footprint Tracker MapReduce Job for PowerGrid AI

Pipeline 5: Monitor environmental impact of power generation.
Map: (Plant_Type, MW) -> (Plant_Type, CO2_Tons)
Reduce: (Plant_Type, [CO2_List]) -> (Plant_Type, Total_Emissions)
"""
import os
import sys
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.rdd import RDD

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.spark_config import create_spark_session
from config.grid_parameters import EMISSION_FACTORS
from config.thresholds import (
    EMISSION_CRITICAL,
    EMISSION_WARNING,
    EMISSION_TARGET,
    RENEWABLE_TARGET,
    RENEWABLE_MINIMUM,
)


class CarbonTracker:
    """
    Tracks carbon emissions from power generation using MapReduce.

    Demonstrates MapReduce with transformation logic (emission factors).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        # Broadcast emission factors for efficient distributed access
        self.emission_factors_bc = self.sc.broadcast(EMISSION_FACTORS)

    def load_plant_data(self, file_path: str) -> DataFrame:
        """Load power plant output data."""
        print(f"Loading plant output data from {file_path}...")

        df = self.spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )

        print(f"Loaded {df.count():,} records")
        return df

    def map_phase(self, df: DataFrame) -> RDD:
        """
        MAP PHASE: Transform plant output to emissions.

        Map: (plant_output) -> (plant_type, co2_tons)

        Formula: CO2_tons = output_mw * emission_factor
        (Assuming 1 hour of operation per reading)
        """
        print("\n" + "=" * 60)
        print("CARBON TRACKING - MapReduce Pipeline")
        print("=" * 60)

        emission_factors = self.emission_factors_bc.value

        print("MAP: (plant_output) -> (plant_type, co2_tons)")
        print(f"  Using emission factors: {emission_factors}")

        def calculate_emissions(row):
            plant_type = row.plant_type
            output_mw = row.output_mw

            # Get emission factor (tons CO2 per MWh)
            factor = emission_factors.get(plant_type, 0.5)  # Default to 0.5

            # Calculate emissions (assuming 1 hour operation)
            co2_tons = output_mw * factor

            return (plant_type, co2_tons)

        mapped_rdd = df.rdd.map(calculate_emissions)
        return mapped_rdd

    def reduce_phase(self, mapped_rdd: RDD) -> RDD:
        """
        REDUCE PHASE: Sum emissions by plant type.
        """
        print("REDUCE: (plant_type, [co2_list]) -> (plant_type, total_emissions)")

        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)
        return reduced_rdd

    def track_emissions(self, df: DataFrame) -> DataFrame:
        """
        Execute the full carbon tracking pipeline.
        """
        # MAP
        mapped_rdd = self.map_phase(df)

        # REDUCE
        reduced_rdd = self.reduce_phase(mapped_rdd)

        # Convert to DataFrame
        result_df = reduced_rdd.toDF(["plant_type", "total_co2_tons"])
        result_df = result_df.orderBy("plant_type")

        return result_df

    def track_emissions_by_district(self, df: DataFrame) -> DataFrame:
        """
        Track emissions by district.

        Map: (plant_output) -> (district_id, co2_tons)
        Reduce: (district_id, [co2_list]) -> (district_id, total)
        """
        print("\n" + "=" * 60)
        print("DISTRICT EMISSIONS - MapReduce Pipeline")
        print("=" * 60)

        emission_factors = self.emission_factors_bc.value

        # MAP
        print("MAP: (plant_output) -> (district_id, co2_tons)")
        mapped_rdd = df.rdd.map(
            lambda row: (
                row.district_id,
                row.output_mw * emission_factors.get(row.plant_type, 0.5)
            )
        )

        # REDUCE
        print("REDUCE: (district_id, [co2_list]) -> (district_id, total)")
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        result_df = reduced_rdd.toDF(["district_id", "total_co2_tons"])
        return result_df.orderBy("district_id")

    def track_emissions_hourly(self, df: DataFrame) -> DataFrame:
        """
        Track emissions by hour for time-series analysis.

        Map: (plant_output) -> (hour, co2_tons)
        Reduce: (hour, [co2_list]) -> (hour, total)
        """
        print("\n" + "=" * 60)
        print("HOURLY EMISSIONS - MapReduce Pipeline")
        print("=" * 60)

        emission_factors = self.emission_factors_bc.value

        # MAP with hour key
        print("MAP: (plant_output) -> (hour, co2_tons)")
        mapped_rdd = df.rdd.map(
            lambda row: (
                row.timestamp.hour,
                row.output_mw * emission_factors.get(row.plant_type, 0.5)
            )
        )

        # REDUCE
        print("REDUCE: (hour, [co2_list]) -> (hour, total)")
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        result_df = reduced_rdd.toDF(["hour", "total_co2_tons"])
        return result_df.orderBy("hour")

    def calculate_renewable_percentage(self, df: DataFrame) -> Dict:
        """
        Calculate renewable vs non-renewable generation percentages.

        Uses MapReduce to categorize and sum by renewable status.
        """
        print("\nCalculating renewable energy percentage...")

        renewable_types = {"solar", "wind", "hydro"}

        # MAP: Categorize and extract output
        mapped_rdd = df.rdd.map(
            lambda row: (
                "renewable" if row.plant_type in renewable_types else "non_renewable",
                row.output_mw
            )
        )

        # REDUCE
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        results = dict(reduced_rdd.collect())

        renewable = results.get("renewable", 0)
        non_renewable = results.get("non_renewable", 0)
        total = renewable + non_renewable

        renewable_pct = (renewable / total * 100) if total > 0 else 0

        return {
            "renewable_mw": renewable,
            "non_renewable_mw": non_renewable,
            "total_mw": total,
            "renewable_percentage": renewable_pct,
            "meets_target": renewable_pct >= RENEWABLE_TARGET * 100,
            "meets_minimum": renewable_pct >= RENEWABLE_MINIMUM * 100,
        }

    def calculate_emission_intensity(self, emissions_df: DataFrame, supply_df: DataFrame) -> float:
        """
        Calculate overall emission intensity (tons CO2 per MWh).
        """
        total_emissions = emissions_df.agg(F.sum("total_co2_tons")).collect()[0][0] or 0
        total_supply = supply_df.agg(F.sum("total_supply_mw")).collect()[0][0] or 0

        if total_supply > 0:
            intensity = total_emissions / total_supply
        else:
            intensity = 0

        return intensity

    def get_carbon_summary(
        self,
        emissions_df: DataFrame,
        renewable_stats: Dict,
        emission_intensity: float,
    ) -> Dict:
        """Generate comprehensive carbon summary."""
        total_emissions = emissions_df.agg(F.sum("total_co2_tons")).collect()[0][0] or 0

        # Determine status
        if emission_intensity >= EMISSION_CRITICAL:
            status = "CRITICAL"
        elif emission_intensity >= EMISSION_WARNING:
            status = "WARNING"
        elif emission_intensity <= EMISSION_TARGET:
            status = "GREEN"
        else:
            status = "OK"

        return {
            "total_emissions_tons": total_emissions,
            "emission_intensity": emission_intensity,
            "emission_status": status,
            "renewable_stats": renewable_stats,
        }

    def save_results(self, df: DataFrame, output_path: str, name: str):
        """Save results to CSV."""
        print(f"Saving {name} to {output_path}...")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


def run_carbon_tracking(
    spark: SparkSession,
    plant_path: str,
    supply_results: Dict,
    output_dir: str,
) -> Dict:
    """
    Run the complete carbon tracking pipeline.
    """
    tracker = CarbonTracker(spark)

    # Load data
    plant_df = tracker.load_plant_data(plant_path)

    # Track emissions
    emissions_by_type = tracker.track_emissions(plant_df)
    emissions_by_district = tracker.track_emissions_by_district(plant_df)
    emissions_hourly = tracker.track_emissions_hourly(plant_df)

    # Calculate renewable percentage
    renewable_stats = tracker.calculate_renewable_percentage(plant_df)

    # Calculate emission intensity
    emission_intensity = tracker.calculate_emission_intensity(
        emissions_by_type,
        supply_results["total_supply"]
    )

    # Get summary
    carbon_summary = tracker.get_carbon_summary(
        emissions_by_type,
        renewable_stats,
        emission_intensity
    )

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    tracker.save_results(emissions_by_type, os.path.join(output_dir, "emissions_by_type"), "emissions by type")
    tracker.save_results(emissions_by_district, os.path.join(output_dir, "emissions_by_district"), "emissions by district")
    tracker.save_results(emissions_hourly, os.path.join(output_dir, "emissions_hourly"), "hourly emissions")

    # Print summary
    print("\n" + "=" * 60)
    print("CARBON TRACKING RESULTS")
    print("=" * 60)

    print("\nEmissions by Plant Type:")
    emissions_by_type.show(truncate=False)

    print("\nEmissions by District:")
    emissions_by_district.show(10, truncate=False)

    print("\nHourly Emissions Pattern:")
    emissions_hourly.show(24, truncate=False)

    print("\nCarbon Summary:")
    print(f"  Total Emissions: {carbon_summary['total_emissions_tons']:,.2f} tons CO2")
    print(f"  Emission Intensity: {carbon_summary['emission_intensity']:.4f} tons/MWh")
    print(f"  Status: {carbon_summary['emission_status']}")

    print("\nRenewable Energy:")
    print(f"  Renewable Generation: {renewable_stats['renewable_mw']:,.2f} MW")
    print(f"  Non-Renewable: {renewable_stats['non_renewable_mw']:,.2f} MW")
    print(f"  Renewable Percentage: {renewable_stats['renewable_percentage']:.1f}%")
    print(f"  Meets 40% Target: {'Yes' if renewable_stats['meets_target'] else 'No'}")
    print(f"  Meets 20% Minimum: {'Yes' if renewable_stats['meets_minimum'] else 'No'}")

    return {
        "emissions_by_type": emissions_by_type,
        "emissions_by_district": emissions_by_district,
        "emissions_hourly": emissions_hourly,
        "renewable_stats": renewable_stats,
        "summary": carbon_summary,
    }


def main():
    """Main function for standalone execution."""
    # This requires supply results - run from main.py
    print("Carbon Tracker should be run from main.py with supply results.")
    print("Run: python src/main.py")


if __name__ == "__main__":
    main()
