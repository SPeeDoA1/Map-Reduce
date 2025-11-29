"""
Chart Generation for PowerGrid AI

Creates matplotlib visualizations for analytics reports.
"""
import os
import sys
from typing import Dict, List, Optional
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class ChartGenerator:
    """Generates various charts for PowerGrid AI analytics."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        self.colors = {
            "primary": "#2196F3",
            "secondary": "#4CAF50",
            "warning": "#FFC107",
            "danger": "#F44336",
            "info": "#00BCD4",
            "nuclear": "#9C27B0",
            "coal": "#795548",
            "gas": "#FF9800",
            "hydro": "#03A9F4",
            "wind": "#8BC34A",
            "solar": "#FFEB3B",
        }

    def plot_demand_by_district(self, demand_df, save: bool = True) -> Optional[str]:
        """Create bar chart of demand by district."""
        fig, ax = plt.subplots(figsize=(14, 6))

        data = demand_df.toPandas().sort_values("total_demand_kwh", ascending=True)

        bars = ax.barh(data["district_id"], data["total_demand_kwh"], color=self.colors["primary"])

        ax.set_xlabel("Total Demand (kWh)", fontsize=12)
        ax.set_ylabel("District", fontsize=12)
        ax.set_title("Energy Demand by District", fontsize=14, fontweight="bold")

        # Add value labels
        for bar, val in zip(bars, data["total_demand_kwh"]):
            ax.text(val + max(data["total_demand_kwh"]) * 0.01, bar.get_y() + bar.get_height()/2,
                   f'{val:,.0f}', va='center', fontsize=8)

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "demand_by_district.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_demand_by_type(self, type_df, save: bool = True) -> Optional[str]:
        """Create pie chart of demand by meter type."""
        fig, ax = plt.subplots(figsize=(10, 8))

        data = type_df.toPandas()

        colors = [self.colors["primary"], self.colors["secondary"], self.colors["warning"]]
        explode = [0.05] * len(data)

        wedges, texts, autotexts = ax.pie(
            data["total_demand_kwh"],
            labels=data["meter_type"].str.title(),
            autopct='%1.1f%%',
            explode=explode,
            colors=colors[:len(data)],
            shadow=True,
            startangle=90
        )

        ax.set_title("Energy Demand by Meter Type", fontsize=14, fontweight="bold")

        # Add legend with values
        legend_labels = [f'{row.meter_type.title()}: {row.total_demand_kwh:,.0f} kWh'
                        for row in data.itertuples()]
        ax.legend(wedges, legend_labels, loc="lower right")

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "demand_by_type.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_supply_by_type(self, type_df, save: bool = True) -> Optional[str]:
        """Create bar chart of supply by plant type."""
        fig, ax = plt.subplots(figsize=(12, 6))

        data = type_df.toPandas().sort_values("total_supply_mw", ascending=False)

        colors = [self.colors.get(t, self.colors["primary"]) for t in data["plant_type"]]
        bars = ax.bar(data["plant_type"].str.title(), data["total_supply_mw"], color=colors)

        ax.set_xlabel("Plant Type", fontsize=12)
        ax.set_ylabel("Total Generation (MW)", fontsize=12)
        ax.set_title("Power Generation by Plant Type", fontsize=14, fontweight="bold")

        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:,.0f}',
                   ha='center', va='bottom', fontsize=10)

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "supply_by_type.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_capacity_utilization(self, utilization_df, save: bool = True) -> Optional[str]:
        """Create horizontal bar chart of capacity utilization."""
        fig, ax = plt.subplots(figsize=(12, 6))

        data = utilization_df.toPandas().sort_values("utilization_rate", ascending=True)

        colors = [self.colors.get(t, self.colors["primary"]) for t in data["plant_type"]]
        bars = ax.barh(data["plant_type"].str.title(), data["utilization_rate"] * 100, color=colors)

        ax.set_xlabel("Utilization Rate (%)", fontsize=12)
        ax.set_ylabel("Plant Type", fontsize=12)
        ax.set_title("Capacity Utilization by Plant Type", fontsize=14, fontweight="bold")
        ax.set_xlim(0, 100)

        # Add reference lines
        ax.axvline(x=80, color='red', linestyle='--', alpha=0.5, label='High (80%)')
        ax.axvline(x=50, color='orange', linestyle='--', alpha=0.5, label='Medium (50%)')

        # Add value labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 1, bar.get_y() + bar.get_height()/2.,
                   f'{width:.1f}%',
                   ha='left', va='center', fontsize=10)

        ax.legend(loc='lower right')
        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "capacity_utilization.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_grid_balance(self, balance_df, save: bool = True) -> Optional[str]:
        """Create bar chart of grid balance by district."""
        fig, ax = plt.subplots(figsize=(14, 6))

        data = balance_df.toPandas().sort_values("net_load_mw")

        # Color by status
        status_colors = {
            "CRITICAL": self.colors["danger"],
            "WARNING": self.colors["warning"],
            "OK": self.colors["secondary"],
            "NORMAL": self.colors["secondary"],
            "EXCESS": self.colors["info"],
        }
        colors = [status_colors.get(s, self.colors["primary"]) for s in data["status"]]

        bars = ax.bar(data["district_id"], data["net_load_mw"], color=colors)

        ax.set_xlabel("District", fontsize=12)
        ax.set_ylabel("Net Load (MW) [Supply - Demand]", fontsize=12)
        ax.set_title("Grid Balance by District", fontsize=14, fontweight="bold")
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)

        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')

        # Add legend
        legend_patches = [mpatches.Patch(color=c, label=s) for s, c in status_colors.items()]
        ax.legend(handles=legend_patches, loc='upper left')

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "grid_balance.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_fraud_zscore_distribution(self, zscore_df, save: bool = True) -> Optional[str]:
        """Create histogram of Z-score distribution."""
        fig, ax = plt.subplots(figsize=(12, 6))

        data = zscore_df.toPandas()

        # Clip extreme values for better visualization
        zscores = data["zscore"].clip(-5, 5)

        ax.hist(zscores, bins=50, color=self.colors["primary"], edgecolor='white', alpha=0.7)

        # Add threshold lines
        ax.axvline(x=3, color='red', linestyle='--', linewidth=2, label='Fraud Threshold (+3)')
        ax.axvline(x=-3, color='red', linestyle='--', linewidth=2, label='Fraud Threshold (-3)')
        ax.axvline(x=0, color='black', linestyle='-', linewidth=1)

        ax.set_xlabel("Z-Score", fontsize=12)
        ax.set_ylabel("Number of Meters", fontsize=12)
        ax.set_title("Meter Usage Z-Score Distribution", fontsize=14, fontweight="bold")
        ax.legend()

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "fraud_zscore_dist.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_fraud_by_status(self, zscore_df, save: bool = True) -> Optional[str]:
        """Create bar chart of fraud status distribution."""
        fig, ax = plt.subplots(figsize=(10, 6))

        data = zscore_df.groupBy("status").count().toPandas()

        status_colors = {
            "NORMAL": self.colors["secondary"],
            "MONITOR": self.colors["info"],
            "SUSPICIOUS": self.colors["warning"],
            "FRAUD_LIKELY": self.colors["danger"],
        }
        colors = [status_colors.get(s, self.colors["primary"]) for s in data["status"]]

        bars = ax.bar(data["status"], data["count"], color=colors)

        ax.set_xlabel("Status", fontsize=12)
        ax.set_ylabel("Number of Meters", fontsize=12)
        ax.set_title("Meter Status Distribution", fontsize=14, fontweight="bold")

        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom', fontsize=10)

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "fraud_status_dist.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_emissions_by_type(self, emissions_df, save: bool = True) -> Optional[str]:
        """Create pie chart of emissions by plant type."""
        fig, ax = plt.subplots(figsize=(10, 8))

        data = emissions_df.toPandas()
        data = data[data["total_co2_tons"] > 0]  # Filter zero emissions

        colors = [self.colors.get(t, self.colors["primary"]) for t in data["plant_type"]]

        wedges, texts, autotexts = ax.pie(
            data["total_co2_tons"],
            labels=data["plant_type"].str.title(),
            autopct='%1.1f%%',
            colors=colors,
            shadow=True,
            startangle=90
        )

        ax.set_title("CO2 Emissions by Plant Type", fontsize=14, fontweight="bold")

        # Add legend with values
        legend_labels = [f'{row.plant_type.title()}: {row.total_co2_tons:,.0f} tons'
                        for row in data.itertuples()]
        ax.legend(wedges, legend_labels, loc="lower right")

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "emissions_by_type.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_hourly_emissions(self, hourly_df, save: bool = True) -> Optional[str]:
        """Create line chart of hourly emissions."""
        fig, ax = plt.subplots(figsize=(12, 6))

        data = hourly_df.toPandas().sort_values("hour")

        ax.plot(data["hour"], data["total_co2_tons"], color=self.colors["danger"],
               linewidth=2, marker='o', markersize=6)
        ax.fill_between(data["hour"], data["total_co2_tons"], alpha=0.3, color=self.colors["danger"])

        ax.set_xlabel("Hour of Day", fontsize=12)
        ax.set_ylabel("CO2 Emissions (tons)", fontsize=12)
        ax.set_title("Hourly CO2 Emissions Pattern", fontsize=14, fontweight="bold")
        ax.set_xticks(range(0, 24))

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "hourly_emissions.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def plot_renewable_mix(self, renewable_stats: Dict, save: bool = True) -> Optional[str]:
        """Create pie chart of renewable vs non-renewable."""
        fig, ax = plt.subplots(figsize=(10, 8))

        values = [renewable_stats["renewable_mw"], renewable_stats["non_renewable_mw"]]
        labels = ["Renewable", "Non-Renewable"]
        colors = [self.colors["secondary"], self.colors["coal"]]

        wedges, texts, autotexts = ax.pie(
            values,
            labels=labels,
            autopct='%1.1f%%',
            colors=colors,
            shadow=True,
            startangle=90,
            explode=[0.05, 0]
        )

        ax.set_title("Energy Mix: Renewable vs Non-Renewable", fontsize=14, fontweight="bold")

        # Target indicator
        target_text = f"Target: 40%\nCurrent: {renewable_stats['renewable_percentage']:.1f}%"
        status = "✅ Met" if renewable_stats["meets_target"] else "❌ Not Met"
        ax.text(0, -1.3, f"{target_text}\n{status}", ha='center', fontsize=12)

        plt.tight_layout()

        if save:
            path = os.path.join(self.output_dir, "renewable_mix.png")
            plt.savefig(path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved: {path}")
            return path
        else:
            plt.show()
            return None

    def generate_all_charts(
        self,
        demand_results: Dict,
        supply_results: Dict,
        balance_results: Dict,
        fraud_results: Dict,
        carbon_results: Dict,
    ) -> List[str]:
        """Generate all charts and return list of file paths."""
        print("\n" + "=" * 60)
        print("Generating Charts")
        print("=" * 60)

        paths = []

        # Demand charts
        paths.append(self.plot_demand_by_district(demand_results["total_demand"]))
        paths.append(self.plot_demand_by_type(demand_results["type_demand"]))

        # Supply charts
        paths.append(self.plot_supply_by_type(supply_results["type_supply"]))
        paths.append(self.plot_capacity_utilization(supply_results["utilization"]))

        # Balance charts
        paths.append(self.plot_grid_balance(balance_results["balanced"]))

        # Fraud charts
        paths.append(self.plot_fraud_zscore_distribution(fraud_results["zscores"]))
        paths.append(self.plot_fraud_by_status(fraud_results["zscores"]))

        # Carbon charts
        paths.append(self.plot_emissions_by_type(carbon_results["emissions_by_type"]))
        paths.append(self.plot_hourly_emissions(carbon_results["emissions_hourly"]))
        paths.append(self.plot_renewable_mix(carbon_results["renewable_stats"]))

        print(f"\nGenerated {len(paths)} charts in {self.output_dir}")
        return [p for p in paths if p]


def main():
    """Demo chart generation."""
    print("Chart Generator module loaded. Run from main.py to generate charts.")


if __name__ == "__main__":
    main()
