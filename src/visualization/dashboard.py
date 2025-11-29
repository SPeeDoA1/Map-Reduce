"""
Console Dashboard for PowerGrid AI

Real-time metrics and alerts display using Rich library.
"""
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.thresholds import SEVERITY_LEVELS


class Dashboard:
    """Console-based dashboard for PowerGrid AI monitoring."""

    def __init__(self):
        if RICH_AVAILABLE:
            self.console = Console()
        else:
            self.console = None

    def _print_simple(self, text: str):
        """Simple print fallback when Rich is not available."""
        print(text)

    def display_header(self):
        """Display the dashboard header."""
        header = """
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║   ⚡ PowerGrid AI - Distributed Energy Intelligence Platform                  ║
║                                                                               ║
║   Enterprise-Scale MapReduce System for Grid Analytics                        ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
        """
        if RICH_AVAILABLE:
            self.console.print(Panel(
                "[bold cyan]PowerGrid AI[/bold cyan]\n"
                "[dim]Distributed Energy Intelligence Platform[/dim]\n"
                f"[dim]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
                title="⚡ Dashboard",
                border_style="cyan"
            ))
        else:
            print(header)
            print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    def display_demand_summary(self, demand_results: Dict):
        """Display demand aggregation summary."""
        if RICH_AVAILABLE:
            table = Table(title="📊 Demand Summary", box=box.ROUNDED)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")

            # Get total demand
            total_demand = demand_results["total_demand"]
            total_kwh = total_demand.agg({"total_demand_kwh": "sum"}).collect()[0][0]

            # Get type breakdown
            type_demand = demand_results["type_demand"].collect()

            table.add_row("Total Demand", f"{total_kwh:,.2f} kWh")
            table.add_row("Districts Monitored", str(total_demand.count()))

            for row in type_demand:
                table.add_row(f"  {row.meter_type.title()}", f"{row.total_demand_kwh:,.2f} kWh")

            self.console.print(table)
        else:
            print("=" * 50)
            print("DEMAND SUMMARY")
            print("=" * 50)
            total_demand = demand_results["total_demand"]
            total_kwh = total_demand.agg({"total_demand_kwh": "sum"}).collect()[0][0]
            print(f"Total Demand: {total_kwh:,.2f} kWh")
            print(f"Districts: {total_demand.count()}")
            print()

    def display_supply_summary(self, supply_results: Dict):
        """Display supply optimization summary."""
        if RICH_AVAILABLE:
            table = Table(title="🏭 Supply Summary", box=box.ROUNDED)
            table.add_column("Plant Type", style="cyan")
            table.add_column("Generation (MW)", style="green")
            table.add_column("Utilization", style="yellow")

            # Get utilization data
            utilization = supply_results["utilization"].collect()

            total_mw = 0
            for row in utilization:
                util_pct = row.utilization_rate * 100
                table.add_row(
                    row.plant_type.title(),
                    f"{row.total_output_mw:,.2f}",
                    f"{util_pct:.1f}%"
                )
                total_mw += row.total_output_mw

            table.add_row("─" * 12, "─" * 15, "─" * 10)
            table.add_row("[bold]Total[/bold]", f"[bold]{total_mw:,.2f}[/bold]", "")

            self.console.print(table)
        else:
            print("=" * 50)
            print("SUPPLY SUMMARY")
            print("=" * 50)
            utilization = supply_results["utilization"].collect()
            for row in utilization:
                print(f"{row.plant_type}: {row.total_output_mw:,.2f} MW ({row.utilization_rate*100:.1f}%)")
            print()

    def display_grid_balance(self, balance_results: Dict):
        """Display grid balance status."""
        risk = balance_results["risk_summary"]

        if RICH_AVAILABLE:
            # Status colors
            status_colors = {
                "CRITICAL": "red",
                "WARNING": "yellow",
                "OK": "green",
                "NORMAL": "green",
                "EXCESS": "blue",
            }

            table = Table(title="⚖️ Grid Balance", box=box.ROUNDED)
            table.add_column("Status", style="cyan")
            table.add_column("Districts", style="white")

            for status, count in risk["status_counts"].items():
                color = status_colors.get(status, "white")
                table.add_row(f"[{color}]{status}[/{color}]", str(count))

            self.console.print(table)

            # Alert for critical districts
            if risk["critical_districts"]:
                self.console.print(Panel(
                    f"[bold red]⚠️ CRITICAL ALERT[/bold red]\n\n"
                    f"Districts at blackout risk:\n"
                    f"{', '.join(risk['critical_districts'])}\n\n"
                    f"Total Deficit: {risk['total_deficit_mw']:,.2f} MW",
                    title="🚨 Alert",
                    border_style="red"
                ))
        else:
            print("=" * 50)
            print("GRID BALANCE")
            print("=" * 50)
            for status, count in risk["status_counts"].items():
                print(f"{status}: {count} districts")
            if risk["critical_districts"]:
                print(f"\n⚠️ CRITICAL: {', '.join(risk['critical_districts'])}")
            print()

    def display_fraud_summary(self, fraud_results: Dict):
        """Display fraud detection summary."""
        summary = fraud_results["summary"]

        if RICH_AVAILABLE:
            table = Table(title="🔍 Fraud Detection", box=box.ROUNDED)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Meters Analyzed", f"{summary['total_meters_analyzed']:,}")
            table.add_row("Fraud Cases", f"[red]{summary['total_fraud_cases']}[/red]")
            table.add_row("Fraud Rate", f"{summary['fraud_rate_pct']:.2f}%")
            table.add_row("Est. Stolen Energy", f"{summary['estimated_stolen_kwh']:,.2f} kWh")
            table.add_row("Est. Revenue Loss", f"[red]${summary['estimated_revenue_loss']:,.2f}[/red]")

            self.console.print(table)
        else:
            print("=" * 50)
            print("FRAUD DETECTION")
            print("=" * 50)
            print(f"Meters Analyzed: {summary['total_meters_analyzed']:,}")
            print(f"Fraud Cases: {summary['total_fraud_cases']}")
            print(f"Fraud Rate: {summary['fraud_rate_pct']:.2f}%")
            print(f"Est. Revenue Loss: ${summary['estimated_revenue_loss']:,.2f}")
            print()

    def display_carbon_summary(self, carbon_results: Dict):
        """Display carbon tracking summary."""
        summary = carbon_results["summary"]
        renewable = carbon_results["renewable_stats"]

        if RICH_AVAILABLE:
            # Emissions table
            table = Table(title="🌍 Carbon Footprint", box=box.ROUNDED)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="white")

            status_colors = {
                "CRITICAL": "red",
                "WARNING": "yellow",
                "OK": "green",
                "GREEN": "green",
            }
            status = summary["emission_status"]
            color = status_colors.get(status, "white")

            table.add_row("Total Emissions", f"{summary['total_emissions_tons']:,.2f} tons CO2")
            table.add_row("Emission Intensity", f"{summary['emission_intensity']:.4f} tons/MWh")
            table.add_row("Status", f"[{color}]{status}[/{color}]")

            self.console.print(table)

            # Renewable table
            renew_table = Table(title="♻️ Renewable Energy", box=box.ROUNDED)
            renew_table.add_column("Source", style="cyan")
            renew_table.add_column("Generation", style="green")

            renew_table.add_row("Renewable", f"{renewable['renewable_mw']:,.2f} MW")
            renew_table.add_row("Non-Renewable", f"{renewable['non_renewable_mw']:,.2f} MW")
            renew_table.add_row("Renewable %", f"{renewable['renewable_percentage']:.1f}%")

            target_status = "✅" if renewable["meets_target"] else "❌"
            renew_table.add_row("40% Target", target_status)

            self.console.print(renew_table)
        else:
            print("=" * 50)
            print("CARBON FOOTPRINT")
            print("=" * 50)
            print(f"Total Emissions: {summary['total_emissions_tons']:,.2f} tons CO2")
            print(f"Emission Intensity: {summary['emission_intensity']:.4f} tons/MWh")
            print(f"Status: {summary['emission_status']}")
            print(f"\nRenewable: {renewable['renewable_percentage']:.1f}%")
            print()

    def display_full_dashboard(
        self,
        demand_results: Dict,
        supply_results: Dict,
        balance_results: Dict,
        fraud_results: Dict,
        carbon_results: Dict,
    ):
        """Display the complete dashboard."""
        if RICH_AVAILABLE:
            self.console.clear()

        self.display_header()
        print()

        self.display_demand_summary(demand_results)
        print()

        self.display_supply_summary(supply_results)
        print()

        self.display_grid_balance(balance_results)
        print()

        self.display_fraud_summary(fraud_results)
        print()

        self.display_carbon_summary(carbon_results)

        if RICH_AVAILABLE:
            self.console.print("\n[dim]Dashboard generated at "
                             f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]")
        else:
            print(f"\nGenerated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def display_processing_status(self, stage: str, status: str = "running"):
        """Display processing status for a pipeline stage."""
        icons = {
            "running": "⏳",
            "complete": "✅",
            "error": "❌",
        }
        icon = icons.get(status, "•")

        if RICH_AVAILABLE:
            color = "yellow" if status == "running" else "green" if status == "complete" else "red"
            self.console.print(f"[{color}]{icon} {stage}[/{color}]")
        else:
            print(f"{icon} {stage}")


def main():
    """Demo the dashboard with sample data."""
    print("Dashboard module loaded. Run from main.py to see full dashboard.")


if __name__ == "__main__":
    main()
