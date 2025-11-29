"""
Tests for data generation modules.
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_generation.smart_meter_simulator import SmartMeterSimulator
from src.data_generation.weather_simulator import WeatherSimulator
from src.data_generation.power_plant_simulator import PowerPlantSimulator


class TestSmartMeterSimulator:
    """Tests for SmartMeterSimulator."""

    def test_create_districts(self):
        """Test district creation."""
        sim = SmartMeterSimulator(num_districts=5)
        assert len(sim.districts) == 5
        for d in sim.districts:
            assert "district_id" in d
            assert "district_type" in d
            assert "num_meters" in d

    def test_create_meters(self):
        """Test meter creation."""
        sim = SmartMeterSimulator(num_districts=2)
        assert len(sim.meters) > 0
        for m in sim.meters:
            assert "meter_id" in m
            assert "district_id" in m
            assert "meter_type" in m
            assert "base_consumption" in m

    def test_meter_types(self):
        """Test that meter types are valid."""
        sim = SmartMeterSimulator(num_districts=2)
        valid_types = {"residential", "commercial", "industrial"}
        for m in sim.meters:
            assert m["meter_type"] in valid_types


class TestWeatherSimulator:
    """Tests for WeatherSimulator."""

    def test_generate_weather(self):
        """Test weather data generation."""
        sim = WeatherSimulator()
        df = sim.generate_weather_data(num_days=1, readings_per_day=24)
        assert len(df) == 24
        assert "timestamp" in df.columns
        assert "cloud_factor" in df.columns
        assert "wind_factor" in df.columns

    def test_weather_factors_range(self):
        """Test that weather factors are in valid range."""
        sim = WeatherSimulator()
        df = sim.generate_weather_data(num_days=1)
        assert df["cloud_factor"].min() >= 0
        assert df["cloud_factor"].max() <= 1
        assert df["wind_factor"].min() >= 0
        assert df["wind_factor"].max() <= 1


class TestPowerPlantSimulator:
    """Tests for PowerPlantSimulator."""

    def test_create_plants(self):
        """Test plant creation."""
        sim = PowerPlantSimulator(num_plants=10)
        assert len(sim.plants) == 10
        for p in sim.plants:
            assert "plant_id" in p
            assert "plant_type" in p
            assert "capacity_mw" in p

    def test_plant_types(self):
        """Test that plant types are valid."""
        sim = PowerPlantSimulator(num_plants=20)
        valid_types = {"nuclear", "coal", "gas", "hydro", "wind", "solar"}
        for p in sim.plants:
            assert p["plant_type"] in valid_types


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
