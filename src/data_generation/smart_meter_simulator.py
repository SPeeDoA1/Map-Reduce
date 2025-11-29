"""
Smart Meter Data Simulator for PowerGrid AI

Generates realistic smart meter readings for 100,000+ meters across multiple districts.
"""
import os
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
from faker import Faker

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.grid_parameters import (
    NUM_DISTRICTS,
    DISTRICT_TYPES,
    SIMULATION_DAYS,
    READINGS_PER_DAY,
    BASE_YEAR,
    BASE_MONTH,
)

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


class SmartMeterSimulator:
    """Simulates smart meter data for residential, commercial, and industrial meters."""

    def __init__(self, num_districts: int = NUM_DISTRICTS):
        self.num_districts = num_districts
        self.districts = self._create_districts()
        self.meters = self._create_meters()

    def _create_districts(self) -> List[Dict]:
        """Create district metadata."""
        districts = []
        district_type_list = list(DISTRICT_TYPES.keys())

        for i in range(self.num_districts):
            district_type = district_type_list[i % len(district_type_list)]
            config = DISTRICT_TYPES[district_type]

            districts.append({
                "district_id": f"D{i:03d}",
                "district_name": fake.city(),
                "district_type": district_type,
                "base_demand_factor": config["base_demand_factor"],
                "peak_shift": config["peak_shift"],
                "num_meters": random.randint(*config["meters_range"]),
            })

        return districts

    def _create_meters(self) -> List[Dict]:
        """Create meter metadata for all districts."""
        meters = []

        for district in self.districts:
            for j in range(district["num_meters"]):
                # Determine meter type based on district type
                if district["district_type"] == "industrial":
                    meter_type = random.choices(
                        ["industrial", "commercial"],
                        weights=[0.7, 0.3]
                    )[0]
                elif district["district_type"] == "commercial":
                    meter_type = random.choices(
                        ["commercial", "residential"],
                        weights=[0.6, 0.4]
                    )[0]
                elif district["district_type"] == "residential":
                    meter_type = random.choices(
                        ["residential", "commercial"],
                        weights=[0.9, 0.1]
                    )[0]
                else:  # mixed
                    meter_type = random.choices(
                        ["residential", "commercial", "industrial"],
                        weights=[0.5, 0.35, 0.15]
                    )[0]

                # Base consumption varies by meter type (kWh per hour)
                base_consumption = {
                    "residential": np.random.normal(1.5, 0.5),
                    "commercial": np.random.normal(5.0, 1.5),
                    "industrial": np.random.normal(25.0, 8.0),
                }[meter_type]

                # Flag some meters as potentially fraudulent (2% of meters)
                is_fraud = random.random() < 0.02

                meters.append({
                    "meter_id": f"M{district['district_id']}_{j:05d}",
                    "district_id": district["district_id"],
                    "meter_type": meter_type,
                    "base_consumption": max(0.1, base_consumption),
                    "is_fraud": is_fraud,
                    "address": fake.street_address(),
                })

        return meters

    def _get_demand_factor(self, hour: int, day_of_week: int, meter_type: str) -> float:
        """Calculate demand factor based on time and meter type."""
        # Base hourly pattern (normalized to 1.0 average)
        hourly_pattern = {
            "residential": [
                0.4, 0.3, 0.3, 0.3, 0.4, 0.6,  # 0-5 (night)
                0.9, 1.2, 1.0, 0.7, 0.6, 0.7,  # 6-11 (morning)
                0.8, 0.7, 0.7, 0.8, 1.0, 1.4,  # 12-17 (afternoon)
                1.6, 1.5, 1.3, 1.0, 0.7, 0.5,  # 18-23 (evening)
            ],
            "commercial": [
                0.3, 0.3, 0.3, 0.3, 0.3, 0.4,  # 0-5
                0.6, 0.9, 1.3, 1.5, 1.5, 1.4,  # 6-11
                1.3, 1.4, 1.5, 1.5, 1.4, 1.2,  # 12-17
                0.8, 0.5, 0.4, 0.3, 0.3, 0.3,  # 18-23
            ],
            "industrial": [
                0.8, 0.8, 0.8, 0.8, 0.8, 0.9,  # 0-5 (24/7 operation)
                1.0, 1.2, 1.3, 1.3, 1.3, 1.2,  # 6-11
                1.1, 1.2, 1.3, 1.3, 1.2, 1.1,  # 12-17
                1.0, 0.9, 0.9, 0.8, 0.8, 0.8,  # 18-23
            ],
        }

        base_factor = hourly_pattern[meter_type][hour]

        # Weekend adjustment
        if day_of_week >= 5:  # Saturday, Sunday
            if meter_type == "residential":
                base_factor *= 1.1  # More home usage
            elif meter_type == "commercial":
                base_factor *= 0.5  # Less business activity
            else:  # industrial
                base_factor *= 0.7  # Reduced shifts

        return base_factor

    def generate_readings(
        self,
        start_date: datetime = None,
        num_days: int = SIMULATION_DAYS,
        readings_per_day: int = READINGS_PER_DAY,
    ) -> pd.DataFrame:
        """Generate smart meter readings for all meters."""
        if start_date is None:
            start_date = datetime(BASE_YEAR, BASE_MONTH, 1)

        readings = []
        total_readings = len(self.meters) * num_days * readings_per_day
        print(f"Generating {total_readings:,} meter readings...")

        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            day_of_week = current_date.weekday()

            for hour in range(readings_per_day):
                timestamp = current_date + timedelta(hours=hour)

                for meter in self.meters:
                    # Calculate consumption
                    demand_factor = self._get_demand_factor(
                        hour, day_of_week, meter["meter_type"]
                    )

                    # Add random variation (10-20%)
                    variation = np.random.normal(1.0, 0.15)

                    # Calculate base consumption
                    consumption = meter["base_consumption"] * demand_factor * variation

                    # Apply fraud pattern (significantly lower reading)
                    if meter["is_fraud"]:
                        # Fraudulent meters report 20-50% of actual usage
                        consumption *= random.uniform(0.2, 0.5)

                    # Ensure non-negative
                    consumption = max(0.01, consumption)

                    readings.append({
                        "timestamp": timestamp,
                        "meter_id": meter["meter_id"],
                        "district_id": meter["district_id"],
                        "meter_type": meter["meter_type"],
                        "kwh": round(consumption, 4),
                        "voltage": round(np.random.normal(240, 5), 1),
                        "power_factor": round(np.random.uniform(0.85, 0.99), 2),
                    })

            if (day + 1) % 1 == 0:
                print(f"  Day {day + 1}/{num_days} complete...")

        df = pd.DataFrame(readings)
        print(f"Generated {len(df):,} total readings")
        return df

    def save_data(self, output_dir: str) -> Tuple[str, str]:
        """Generate and save meter data to files."""
        os.makedirs(output_dir, exist_ok=True)

        # Save district metadata
        districts_df = pd.DataFrame(self.districts)
        districts_path = os.path.join(output_dir, "districts.csv")
        districts_df.to_csv(districts_path, index=False)
        print(f"Saved {len(districts_df)} districts to {districts_path}")

        # Save meter metadata
        meters_df = pd.DataFrame(self.meters)
        meters_path = os.path.join(output_dir, "meters.csv")
        meters_df.to_csv(meters_path, index=False)
        print(f"Saved {len(meters_df):,} meters to {meters_path}")

        # Generate and save readings
        readings_df = self.generate_readings()
        readings_path = os.path.join(output_dir, "meter_readings.csv")
        readings_df.to_csv(readings_path, index=False)
        print(f"Saved readings to {readings_path}")

        return districts_path, meters_path, readings_path


def main():
    """Main function to generate smart meter data."""
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_dir = os.path.join(project_root, "data", "raw")

    print("=" * 60)
    print("PowerGrid AI - Smart Meter Simulator")
    print("=" * 60)

    simulator = SmartMeterSimulator(num_districts=NUM_DISTRICTS)
    print(f"\nCreated {len(simulator.districts)} districts")
    print(f"Created {len(simulator.meters):,} meters")

    # Show district summary
    print("\nDistrict Summary:")
    for d in simulator.districts[:5]:
        print(f"  {d['district_id']}: {d['district_name']} ({d['district_type']}) - {d['num_meters']:,} meters")
    print("  ...")

    print("\nGenerating readings...")
    simulator.save_data(output_dir)
    print("\nDone!")


if __name__ == "__main__":
    main()
