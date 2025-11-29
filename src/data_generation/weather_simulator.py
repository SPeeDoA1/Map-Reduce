"""
Weather Data Simulator for PowerGrid AI

Generates realistic weather data that affects renewable energy generation.
"""
import os
import random
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
import numpy as np

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.grid_parameters import (
    SOLAR_CLOUD_FACTORS,
    WIND_SPEED_FACTORS,
    HYDRO_PRECIPITATION_FACTORS,
    SIMULATION_DAYS,
    READINGS_PER_DAY,
    BASE_YEAR,
    BASE_MONTH,
)

np.random.seed(42)
random.seed(42)


class WeatherSimulator:
    """Simulates weather conditions affecting energy generation."""

    def __init__(self):
        self.cloud_conditions = list(SOLAR_CLOUD_FACTORS.keys())
        self.wind_conditions = list(WIND_SPEED_FACTORS.keys())
        self.precip_conditions = list(HYDRO_PRECIPITATION_FACTORS.keys())

    def _generate_weather_pattern(self, num_hours: int) -> List[Dict]:
        """Generate a coherent weather pattern over time."""
        weather = []

        # Initialize weather state
        current_cloud = random.choice(self.cloud_conditions)
        current_wind = random.choice(self.wind_conditions)
        current_precip = random.choice(self.precip_conditions)

        # Temperature base (summer month)
        base_temp = 25  # Celsius

        for hour in range(num_hours):
            # Weather changes gradually (10% chance per hour)
            if random.random() < 0.10:
                # Tend to move to adjacent states
                cloud_idx = self.cloud_conditions.index(current_cloud)
                new_idx = cloud_idx + random.choice([-1, 0, 1])
                new_idx = max(0, min(len(self.cloud_conditions) - 1, new_idx))
                current_cloud = self.cloud_conditions[new_idx]

            if random.random() < 0.10:
                wind_idx = self.wind_conditions.index(current_wind)
                new_idx = wind_idx + random.choice([-1, 0, 1])
                new_idx = max(0, min(len(self.wind_conditions) - 1, new_idx))
                current_wind = self.wind_conditions[new_idx]

            if random.random() < 0.05:  # Precipitation changes slower
                precip_idx = self.precip_conditions.index(current_precip)
                new_idx = precip_idx + random.choice([-1, 0, 1])
                new_idx = max(0, min(len(self.precip_conditions) - 1, new_idx))
                current_precip = self.precip_conditions[new_idx]

            # Calculate factors
            cloud_factor = SOLAR_CLOUD_FACTORS[current_cloud]
            wind_factor = WIND_SPEED_FACTORS[current_wind]
            precip_factor = HYDRO_PRECIPITATION_FACTORS[current_precip]

            # Temperature varies by hour and conditions
            hour_of_day = hour % 24
            temp_hour_adjust = -5 + 10 * np.sin((hour_of_day - 6) * np.pi / 12)
            temp_cloud_adjust = -3 if current_cloud in ["cloudy", "overcast", "rainy"] else 0
            temperature = base_temp + temp_hour_adjust + temp_cloud_adjust + np.random.normal(0, 2)

            # Wind speed in m/s
            wind_speed = {
                "calm": np.random.uniform(0, 3),
                "light": np.random.uniform(3, 6),
                "moderate": np.random.uniform(6, 12),
                "strong": np.random.uniform(12, 25),
                "extreme": np.random.uniform(25, 40),
            }[current_wind]

            # Humidity based on conditions
            humidity = {
                "clear": np.random.uniform(30, 50),
                "partly_cloudy": np.random.uniform(40, 60),
                "cloudy": np.random.uniform(50, 70),
                "overcast": np.random.uniform(60, 80),
                "rainy": np.random.uniform(80, 95),
            }[current_cloud]

            weather.append({
                "cloud_condition": current_cloud,
                "wind_condition": current_wind,
                "precipitation_condition": current_precip,
                "cloud_factor": cloud_factor,
                "wind_factor": wind_factor,
                "precipitation_factor": precip_factor,
                "temperature_c": round(temperature, 1),
                "wind_speed_ms": round(wind_speed, 1),
                "humidity_pct": round(humidity, 1),
            })

        return weather

    def generate_weather_data(
        self,
        start_date: datetime = None,
        num_days: int = SIMULATION_DAYS,
        readings_per_day: int = READINGS_PER_DAY,
    ) -> pd.DataFrame:
        """Generate weather data for the simulation period."""
        if start_date is None:
            start_date = datetime(BASE_YEAR, BASE_MONTH, 1)

        total_hours = num_days * readings_per_day
        print(f"Generating weather data for {num_days} days ({total_hours} hours)...")

        weather_pattern = self._generate_weather_pattern(total_hours)

        records = []
        for hour_idx, weather in enumerate(weather_pattern):
            timestamp = start_date + timedelta(hours=hour_idx)

            records.append({
                "timestamp": timestamp,
                **weather
            })

        df = pd.DataFrame(records)
        print(f"Generated {len(df)} weather records")
        return df

    def save_data(self, output_dir: str) -> str:
        """Generate and save weather data."""
        os.makedirs(output_dir, exist_ok=True)

        weather_df = self.generate_weather_data()
        weather_path = os.path.join(output_dir, "weather.csv")
        weather_df.to_csv(weather_path, index=False)
        print(f"Saved weather data to {weather_path}")

        # Print summary
        print("\nWeather Summary:")
        print(f"  Cloud conditions distribution:")
        for cond in self.cloud_conditions:
            count = len(weather_df[weather_df["cloud_condition"] == cond])
            pct = count / len(weather_df) * 100
            print(f"    {cond}: {count} hours ({pct:.1f}%)")

        print(f"\n  Wind conditions distribution:")
        for cond in self.wind_conditions:
            count = len(weather_df[weather_df["wind_condition"] == cond])
            pct = count / len(weather_df) * 100
            print(f"    {cond}: {count} hours ({pct:.1f}%)")

        return weather_path


def main():
    """Main function to generate weather data."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_dir = os.path.join(project_root, "data", "raw")

    print("=" * 60)
    print("PowerGrid AI - Weather Simulator")
    print("=" * 60)

    simulator = WeatherSimulator()
    simulator.save_data(output_dir)
    print("\nDone!")


if __name__ == "__main__":
    main()
