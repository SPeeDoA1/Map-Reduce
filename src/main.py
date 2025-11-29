#!/usr/bin/env python3
"""
PowerGrid AI - Main Orchestrator

Runs the complete distributed energy intelligence pipeline:
1. Generate synthetic data (if needed)
2. Run MapReduce analytics jobs
3. Display dashboard and generate charts
"""
import os
import sys
import argparse
from datetime import datetime

# Fix for Java 17 compatibility - MUST be set before importing pyspark
# Note: These are only needed for Java 17, not required but helpful
java_options = [
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
]
os.environ["SPARK_SUBMIT_OPTS"] = " ".join(java_options)
os.environ["_JAVA_OPTIONS"] = " ".join(java_options)

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import create_spark_session
from src.data_generation.weather_simulator import WeatherSimulator
from src.data_generation.smart_meter_simulator import SmartMeterSimulator
from src.data_generation.power_plant_simulator import PowerPlantSimulator
from src.spark_jobs.demand_aggregator import run_demand_aggregation
from src.spark_jobs.supply_optimizer import run_supply_optimization
from src.spark_jobs.grid_balancer import run_grid_balancing
from src.spark_jobs.fraud_detector import run_fraud_detection
from src.spark_jobs.carbon_tracker import run_carbon_tracking
from src.visualization.dashboard import Dashboard
from src.visualization.charts import ChartGenerator


def print_banner():
    """Print the application banner."""
    banner = """
    ╔═══════════════════════════════════════════════════════════════════════════╗
    ║                                                                           ║
    ║   ⚡ PowerGrid AI - Distributed Energy Intelligence Platform              ║
    ║                                                                           ║
    ║   Enterprise-Scale MapReduce System for Grid Analytics                    ║
    ║   Powered by Apache Spark                                                 ║
    ║                                                                           ║
    ╚═══════════════════════════════════════════════════════════════════════════╝
    """
    print(banner)
    print(f"    Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def generate_data(data_dir: str, force: bool = False):
    """Generate synthetic data if it doesn't exist."""
    raw_dir = os.path.join(data_dir, "raw")

    # Check if data already exists
    meter_file = os.path.join(raw_dir, "meter_readings.csv")
    if os.path.exists(meter_file) and not force:
        print(f"Data already exists at {raw_dir}")
        print("Use --regenerate to force regeneration")
        return

    print("\n" + "=" * 70)
    print("PHASE 1: DATA GENERATION")
    print("=" * 70)

    # Generate weather first (needed for plant simulation)
    print("\n[1/3] Generating weather data...")
    weather_sim = WeatherSimulator()
    weather_sim.save_data(raw_dir)

    # Generate smart meter data
    print("\n[2/3] Generating smart meter data...")
    meter_sim = SmartMeterSimulator()
    meter_sim.save_data(raw_dir)

    # Generate power plant data (needs weather)
    print("\n[3/3] Generating power plant data...")
    import pandas as pd
    weather_df = pd.read_csv(os.path.join(raw_dir, "weather.csv"), parse_dates=["timestamp"])
    plant_sim = PowerPlantSimulator()
    plant_sim.save_data(raw_dir, weather_df)

    print("\n✅ Data generation complete!")


def run_analytics(spark, data_dir: str):
    """Run all MapReduce analytics jobs."""
    raw_dir = os.path.join(data_dir, "raw")
    processed_dir = os.path.join(data_dir, "processed")

    print("\n" + "=" * 70)
    print("PHASE 2: MAPREDUCE ANALYTICS")
    print("=" * 70)

    results = {}

    # Job 1: Demand Aggregation
    print("\n" + "-" * 50)
    print("JOB 1: DEMAND AGGREGATION")
    print("-" * 50)
    results["demand"] = run_demand_aggregation(
        spark,
        os.path.join(raw_dir, "meter_readings.csv"),
        os.path.join(processed_dir, "demand")
    )

    # Job 2: Supply Optimization
    print("\n" + "-" * 50)
    print("JOB 2: SUPPLY OPTIMIZATION")
    print("-" * 50)
    results["supply"] = run_supply_optimization(
        spark,
        os.path.join(raw_dir, "plant_output.csv"),
        os.path.join(raw_dir, "weather.csv"),
        os.path.join(processed_dir, "supply")
    )

    # Job 3: Grid Balancing
    print("\n" + "-" * 50)
    print("JOB 3: GRID BALANCING")
    print("-" * 50)
    results["balance"] = run_grid_balancing(
        spark,
        results["demand"],
        results["supply"],
        os.path.join(processed_dir, "balance")
    )

    # Job 4: Fraud Detection
    print("\n" + "-" * 50)
    print("JOB 4: FRAUD DETECTION")
    print("-" * 50)
    results["fraud"] = run_fraud_detection(
        spark,
        os.path.join(raw_dir, "meter_readings.csv"),
        os.path.join(processed_dir, "fraud")
    )

    # Job 5: Carbon Tracking
    print("\n" + "-" * 50)
    print("JOB 5: CARBON TRACKING")
    print("-" * 50)
    results["carbon"] = run_carbon_tracking(
        spark,
        os.path.join(raw_dir, "plant_output.csv"),
        results["supply"],
        os.path.join(processed_dir, "carbon")
    )

    print("\n✅ Analytics complete!")
    return results


def display_dashboard(results: dict):
    """Display the console dashboard."""
    print("\n" + "=" * 70)
    print("PHASE 3: DASHBOARD")
    print("=" * 70)

    dashboard = Dashboard()
    dashboard.display_full_dashboard(
        results["demand"],
        results["supply"],
        results["balance"],
        results["fraud"],
        results["carbon"]
    )


def generate_charts(results: dict, output_dir: str):
    """Generate visualization charts."""
    print("\n" + "=" * 70)
    print("PHASE 4: CHART GENERATION")
    print("=" * 70)

    chart_gen = ChartGenerator(output_dir)
    paths = chart_gen.generate_all_charts(
        results["demand"],
        results["supply"],
        results["balance"],
        results["fraud"],
        results["carbon"]
    )

    print(f"\n✅ Generated {len(paths)} charts!")
    return paths


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PowerGrid AI - Distributed Energy Intelligence Platform"
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force regeneration of synthetic data"
    )
    parser.add_argument(
        "--skip-data",
        action="store_true",
        help="Skip data generation (use existing data)"
    )
    parser.add_argument(
        "--skip-charts",
        action="store_true",
        help="Skip chart generation"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Custom data directory path"
    )

    args = parser.parse_args()

    # Setup paths
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = args.data_dir or os.path.join(project_root, "data")
    reports_dir = os.path.join(data_dir, "reports")

    print_banner()

    # Phase 1: Data Generation
    if not args.skip_data:
        generate_data(data_dir, force=args.regenerate)

    # Check if data exists
    meter_file = os.path.join(data_dir, "raw", "meter_readings.csv")
    if not os.path.exists(meter_file):
        print(f"\n❌ Error: Data not found at {meter_file}")
        print("Run without --skip-data to generate data first.")
        sys.exit(1)

    # Phase 2: Analytics
    print("\nInitializing Spark...")
    spark = create_spark_session("PowerGridAI")

    try:
        results = run_analytics(spark, data_dir)

        # Phase 3: Dashboard
        display_dashboard(results)

        # Phase 4: Charts
        if not args.skip_charts:
            generate_charts(results, reports_dir)

        # Summary
        print("\n" + "=" * 70)
        print("EXECUTION COMPLETE")
        print("=" * 70)
        print(f"\n📁 Data directory: {data_dir}")
        print(f"📊 Reports directory: {reports_dir}")
        print(f"⏱️  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    finally:
        spark.stop()
        print("\n✅ Spark session stopped.")


if __name__ == "__main__":
    main()
