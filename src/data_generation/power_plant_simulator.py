"""
Power Plant Data Simulator for PowerGrid AI

Generates realistic power plant output data for various generation types.
"""
import os
import random
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
import numpy as np
from faker import Faker

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.grid_parameters import (
    PLANT_BASE_CAPACITY,
    OPERATING_COSTS,
    NUM_DISTRICTS,
    SIMULATION_DAYS,
    READINGS_PER_DAY,
    BASE_YEAR,
    BASE_MONTH,
)

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


class PowerPlantSimulator:
    """Simulates power plant generation data for various fuel types."""

    def __init__(self, num_plants: int = 50):
        self.num_plants = num_plants
        self.plants = self._create_plants()

    def _create_plants(self) -> List[Dict]:
        """Create power plant metadata."""
        plants = []
        plant_types = list(PLANT_BASE_CAPACITY.keys())

        # Distribution of plant types (realistic grid mix)
        type_distribution = {
            "nuclear": 0.08,    # 4 plants
            "coal": 0.20,       # 10 plants
            "gas": 0.30,        # 15 plants
            "hydro": 0.12,      # 6 plants
            "wind": 0.18,       # 9 plants
            "solar": 0.12,      # 6 plants
        }

        plant_id = 0
        for plant_type, fraction in type_distribution.items():
            num_of_type = max(1, int(self.num_plants * fraction))

            for _ in range(num_of_type):
                if plant_id >= self.num_plants:
                    break

                # Assign to a district (some districts may have multiple plants)
                district_id = f"D{plant_id % NUM_DISTRICTS:03d}"

                # Capacity variation (+/- 30% from base)
                base_cap = PLANT_BASE_CAPACITY[plant_type]
                capacity = base_cap * np.random.uniform(0.7, 1.3)

                # Efficiency factor (how well the plant performs)
                efficiency = np.random.uniform(0.85, 0.98)

                plants.append({
                    "plant_id": f"P{plant_id:03d}",
                    "plant_name": f"{fake.city()} {plant_type.title()} Plant",
                    "plant_type": plant_type,
                    "district_id": district_id,
                    "capacity_mw": round(capacity, 1),
                    "efficiency": round(efficiency, 3),
                    "operating_cost": OPERATING_COSTS[plant_type],
                    "latitude": round(np.random.uniform(25, 48), 4),
                    "longitude": round(np.random.uniform(-125, -70), 4),
                })

                plant_id += 1

        return plants

    def _get_availability_factor(
        self,
        plant_type: str,
        hour: int,
        weather: Dict,
    ) -> float:
        """Calculate plant availability based on type and conditions."""
        # Base availability by type
        base_availability = {
            "nuclear": 0.92,   # Very stable, planned outages
            "coal": 0.85,      # Reliable but needs maintenance
            "gas": 0.90,       # Quick start, flexible
            "hydro": 0.88,     # Depends on water levels
            "wind": 0.35,      # Highly variable
            "solar": 0.25,     # Only during daylight
        }

        availability = base_availability[plant_type]

        # Apply weather effects
        if plant_type == "solar":
            # Solar only works during day and affected by clouds
            if 6 <= hour <= 19:
                cloud_factor = weather.get("cloud_factor", 0.7)
                hour_factor = self._solar_hour_curve(hour)
                availability = hour_factor * cloud_factor
            else:
                availability = 0.0

        elif plant_type == "wind":
            wind_factor = weather.get("wind_factor", 0.5)
            availability *= wind_factor

        elif plant_type == "hydro":
            precip_factor = weather.get("precipitation_factor", 1.0)
            availability *= precip_factor

        # Random maintenance/outage (2% chance per hour)
        if random.random() < 0.02:
            availability *= random.uniform(0.0, 0.5)

        return min(1.0, max(0.0, availability))

    def _solar_hour_curve(self, hour: int) -> float:
        """Solar generation curve based on hour of day."""
        if hour < 6 or hour > 19:
            return 0.0

        # Bell curve peaking at noon
        peak_hour = 12
        spread = 4
        return np.exp(-((hour - peak_hour) ** 2) / (2 * spread ** 2))

    def generate_output(
        self,
        weather_data: pd.DataFrame,
        start_date: datetime = None,
        num_days: int = SIMULATION_DAYS,
        readings_per_day: int = READINGS_PER_DAY,
    ) -> pd.DataFrame:
        """Generate power plant output readings."""
        if start_date is None:
            start_date = datetime(BASE_YEAR, BASE_MONTH, 1)

        outputs = []
        print(f"Generating plant output for {self.num_plants} plants over {num_days} days...")

        for day in range(num_days):
            current_date = start_date + timedelta(days=day)

            for hour in range(readings_per_day):
                timestamp = current_date + timedelta(hours=hour)

                # Get weather for this timestamp
                weather_row = weather_data[
                    weather_data["timestamp"] == timestamp
                ]

                if len(weather_row) > 0:
                    weather = {
                        "cloud_factor": weather_row["cloud_factor"].values[0],
                        "wind_factor": weather_row["wind_factor"].values[0],
                        "precipitation_factor": weather_row["precipitation_factor"].values[0],
                    }
                else:
                    weather = {
                        "cloud_factor": 0.7,
                        "wind_factor": 0.5,
                        "precipitation_factor": 1.0,
                    }

                for plant in self.plants:
                    # Calculate actual output
                    availability = self._get_availability_factor(
                        plant["plant_type"], hour, weather
                    )

                    # Add random variation
                    variation = np.random.normal(1.0, 0.05)

                    # Calculate output in MW
                    output_mw = (
                        plant["capacity_mw"]
                        * availability
                        * plant["efficiency"]
                        * variation
                    )
                    output_mw = max(0, min(plant["capacity_mw"], output_mw))

                    # Calculate cost
                    operating_cost = output_mw * plant["operating_cost"]

                    outputs.append({
                        "timestamp": timestamp,
                        "plant_id": plant["plant_id"],
                        "plant_type": plant["plant_type"],
                        "district_id": plant["district_id"],
                        "capacity_mw": plant["capacity_mw"],
                        "output_mw": round(output_mw, 2),
                        "availability": round(availability, 3),
                        "operating_cost": round(operating_cost, 2),
                    })

            if (day + 1) % 1 == 0:
                print(f"  Day {day + 1}/{num_days} complete...")

        df = pd.DataFrame(outputs)
        print(f"Generated {len(df):,} plant output records")
        return df

    def save_data(self, output_dir: str, weather_data: pd.DataFrame) -> str:
        """Generate and save plant data to files."""
        os.makedirs(output_dir, exist_ok=True)

        # Save plant metadata
        plants_df = pd.DataFrame(self.plants)
        plants_path = os.path.join(output_dir, "plants.csv")
        plants_df.to_csv(plants_path, index=False)
        print(f"Saved {len(plants_df)} plants to {plants_path}")

        # Generate and save output
        output_df = self.generate_output(weather_data)
        output_path = os.path.join(output_dir, "plant_output.csv")
        output_df.to_csv(output_path, index=False)
        print(f"Saved plant output to {output_path}")

        return plants_path, output_path


def main():
    """Main function to generate power plant data."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_dir = os.path.join(project_root, "data", "raw")

    print("=" * 60)
    print("PowerGrid AI - Power Plant Simulator")
    print("=" * 60)

    # Load weather data first
    weather_path = os.path.join(output_dir, "weather.csv")
    if os.path.exists(weather_path):
        weather_data = pd.read_csv(weather_path, parse_dates=["timestamp"])
    else:
        print("Weather data not found. Please run weather_simulator.py first.")
        return

    simulator = PowerPlantSimulator(num_plants=50)
    print(f"\nCreated {len(simulator.plants)} power plants")

    # Show plant summary
    print("\nPlant Summary by Type:")
    plants_df = pd.DataFrame(simulator.plants)
    summary = plants_df.groupby("plant_type").agg({
        "plant_id": "count",
        "capacity_mw": "sum"
    }).rename(columns={"plant_id": "count", "capacity_mw": "total_capacity"})
    print(summary)

    print("\nGenerating output data...")
    simulator.save_data(output_dir, weather_data)
    print("\nDone!")


if __name__ == "__main__":
    main()
