"""
Grid Physics and Economic Parameters for PowerGrid AI
"""

# =============================================================================
# POWER PLANT PARAMETERS
# =============================================================================

# Base capacity in MW for each plant type
PLANT_BASE_CAPACITY = {
    "nuclear": 1000,
    "coal": 600,
    "gas": 400,
    "hydro": 300,
    "wind": 150,
    "solar": 100,
}

# CO2 emissions factor (tons per MWh generated)
EMISSION_FACTORS = {
    "nuclear": 0.0,
    "coal": 1.0,
    "gas": 0.4,
    "hydro": 0.0,
    "wind": 0.0,
    "solar": 0.0,
}

# Operating cost per MWh ($)
OPERATING_COSTS = {
    "nuclear": 30,
    "coal": 40,
    "gas": 50,
    "hydro": 10,
    "wind": 5,
    "solar": 3,
}

# =============================================================================
# WEATHER IMPACT FACTORS
# =============================================================================

# Solar efficiency based on cloud cover (0-1 scale)
SOLAR_CLOUD_FACTORS = {
    "clear": 1.0,
    "partly_cloudy": 0.7,
    "cloudy": 0.3,
    "overcast": 0.1,
    "rainy": 0.05,
}

# Wind turbine efficiency based on wind speed (m/s)
WIND_SPEED_FACTORS = {
    "calm": 0.0,        # < 3 m/s
    "light": 0.3,       # 3-6 m/s
    "moderate": 0.7,    # 6-12 m/s
    "strong": 1.0,      # 12-25 m/s
    "extreme": 0.0,     # > 25 m/s (shutdown for safety)
}

# Hydro efficiency based on precipitation
HYDRO_PRECIPITATION_FACTORS = {
    "drought": 0.4,
    "low": 0.7,
    "normal": 1.0,
    "high": 1.1,
    "flood": 0.8,  # Controlled release
}

# =============================================================================
# GRID TRANSMISSION PARAMETERS
# =============================================================================

# Transmission loss rate per 100km
TRANSMISSION_LOSS_RATE = 0.02  # 2% loss per 100km

# Maximum transmission capacity between districts (MW)
MAX_TRANSMISSION_CAPACITY = 500

# =============================================================================
# PRICING PARAMETERS
# =============================================================================

# Base electricity price ($/kWh)
BASE_ELECTRICITY_PRICE = 0.12

# Peak hour multiplier (6am-9am, 5pm-9pm)
PEAK_MULTIPLIER = 1.5

# Off-peak multiplier (11pm-5am)
OFF_PEAK_MULTIPLIER = 0.7

# Demand surge pricing threshold (% of capacity)
SURGE_THRESHOLD = 0.85

# =============================================================================
# DISTRICT PARAMETERS
# =============================================================================

# Number of districts in the grid
NUM_DISTRICTS = 10

# District types and their characteristics
DISTRICT_TYPES = {
    "industrial": {
        "base_demand_factor": 2.5,
        "peak_shift": 2,  # hours before standard peak
        "meters_range": (100, 300),
    },
    "commercial": {
        "base_demand_factor": 1.5,
        "peak_shift": 0,
        "meters_range": (200, 500),
    },
    "residential": {
        "base_demand_factor": 1.0,
        "peak_shift": 1,  # hours after standard peak
        "meters_range": (500, 1000),
    },
    "mixed": {
        "base_demand_factor": 1.3,
        "peak_shift": 0,
        "meters_range": (300, 600),
    },
}

# =============================================================================
# SIMULATION TIME PARAMETERS
# =============================================================================

# Simulation duration
SIMULATION_DAYS = 3
READINGS_PER_DAY = 24  # Hourly readings

# Base timestamp for simulation
BASE_YEAR = 2024
BASE_MONTH = 6  # June (good for solar/weather variation)
