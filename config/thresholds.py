"""
Alert Thresholds and Critical Limits for PowerGrid AI
"""

# =============================================================================
# GRID STABILITY THRESHOLDS
# =============================================================================

# Net load thresholds (Supply - Demand as % of capacity)
CRITICAL_DEFICIT = -0.15    # Below -15%: Imminent blackout risk
WARNING_DEFICIT = -0.05     # Below -5%: Warning state
NORMAL_RANGE = (0.0, 0.20)  # 0-20% surplus: Normal operation
EXCESS_SURPLUS = 0.30       # Above 30%: Wasted generation

# Frequency deviation limits (Hz from 50/60Hz base)
FREQUENCY_CRITICAL = 0.5    # Grid instability
FREQUENCY_WARNING = 0.2     # Needs correction

# =============================================================================
# FRAUD DETECTION THRESHOLDS
# =============================================================================

# Z-score thresholds for anomaly detection
FRAUD_ZSCORE_CRITICAL = 3.0     # Almost certainly fraud/malfunction
FRAUD_ZSCORE_WARNING = 2.5      # Suspicious, needs investigation
FRAUD_ZSCORE_MONITOR = 2.0      # Worth monitoring

# Minimum samples required for statistical validity
MIN_SAMPLES_FOR_STATS = 30

# Consecutive anomalies before alerting
CONSECUTIVE_ANOMALY_THRESHOLD = 3

# =============================================================================
# DEMAND THRESHOLDS
# =============================================================================

# Peak demand as % of historical max
DEMAND_CRITICAL = 0.95      # Near system capacity
DEMAND_HIGH = 0.85          # Elevated demand
DEMAND_NORMAL = 0.70        # Normal operation

# Sudden demand spike detection (% change in 1 hour)
SPIKE_CRITICAL = 0.25       # 25% increase
SPIKE_WARNING = 0.15        # 15% increase

# =============================================================================
# SUPPLY THRESHOLDS
# =============================================================================

# Plant availability thresholds
SUPPLY_CRITICAL = 0.60      # Below 60% of plants online
SUPPLY_WARNING = 0.75       # Below 75% of plants online

# Reserve margin (surplus capacity as % of demand)
RESERVE_CRITICAL = 0.05     # Only 5% reserve
RESERVE_WARNING = 0.10      # Only 10% reserve
RESERVE_TARGET = 0.20       # Target 20% reserve

# =============================================================================
# CARBON EMISSION THRESHOLDS
# =============================================================================

# Emissions intensity (tons CO2 per MWh)
EMISSION_CRITICAL = 0.8     # High carbon intensity
EMISSION_WARNING = 0.5      # Moderate carbon intensity
EMISSION_TARGET = 0.2       # Green energy target

# Renewable percentage targets
RENEWABLE_TARGET = 0.40     # 40% renewable target
RENEWABLE_MINIMUM = 0.20    # Minimum 20% renewable

# =============================================================================
# FINANCIAL THRESHOLDS
# =============================================================================

# Cost per kWh thresholds ($)
COST_CRITICAL = 0.25        # Very high operating cost
COST_WARNING = 0.18         # Elevated cost
COST_TARGET = 0.12          # Target cost

# Revenue loss thresholds (from fraud/outages)
LOSS_CRITICAL = 100000      # $100k daily loss
LOSS_WARNING = 50000        # $50k daily loss

# =============================================================================
# ALERT SEVERITY LEVELS
# =============================================================================

SEVERITY_LEVELS = {
    "CRITICAL": {
        "priority": 1,
        "color": "red",
        "action": "immediate",
    },
    "WARNING": {
        "priority": 2,
        "color": "yellow",
        "action": "monitor",
    },
    "INFO": {
        "priority": 3,
        "color": "blue",
        "action": "log",
    },
    "OK": {
        "priority": 4,
        "color": "green",
        "action": "none",
    },
}
