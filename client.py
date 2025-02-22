import requests
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

BASE_URL = "http://localhost:7272"
console = Console()


def display_analysis(analysis):
    if not analysis:
        console.print("[bold red]No analysis data available.[/bold red]")
        return

    total_price = analysis.get("total_price", "N/A")
    total_price_ema = analysis.get("total_price_ema", "N/A")
    panel_text = f"[bold cyan]Total Price:[/bold cyan] {total_price}\n" \
                 f"[bold cyan]Total Price EMA:[/bold cyan] {total_price_ema}"
    console.print(Panel(panel_text, title="Global Analysis", style="bold blue"))

    subnet_trends = analysis.get("subnet_gap_trends", [])
    if subnet_trends:
        trend_table = Table(title="Subnet EMA Gap Trends (Top 10)", box=box.DOUBLE_EDGE)
        trend_table.add_column("Netuid", justify="right", style="magenta")
        trend_table.add_column("Current Emission", justify="right", style="yellow")
        trend_table.add_column("EMA5 Price", justify="right", style="green")
        trend_table.add_column("EMA60 Price", justify="right", style="green")
        trend_table.add_column("Gap Price", justify="right", style="cyan")
        trend_table.add_column("Δ Gap Price", justify="right", style="red")
        trend_table.add_column("EMA5 Emission", justify="right", style="green")
        trend_table.add_column("EMA60 Emission", justify="right", style="green")
        trend_table.add_column("Gap Emission", justify="right", style="cyan")
        trend_table.add_column("Δ Gap Emission", justify="right", style="red")
        for trend in subnet_trends:
            trend_table.add_row(
                str(trend.get("netuid", "N/A")),
                f'{trend.get("current_emission", "N/A")}',
                f'{trend.get("final_ema5_price", "N/A")}',
                f'{trend.get("final_ema60_price", "N/A")}',
                f'{trend.get("final_gap_price", "N/A")}',
                f'{trend.get("delta_gap_price", "N/A")}',
                f'{trend.get("final_ema5_emission", "N/A")}',
                f'{trend.get("final_ema60_emission", "N/A")}',
                f'{trend.get("final_gap_emission", "N/A")}',
                f'{trend.get("delta_gap_emission", "N/A")}',
            )
        console.print(trend_table)
    else:
        console.print("[bold red]No subnet gap trend analysis available.[/bold red]")


def log_details():
    url = f"{BASE_URL}/subnets"
    try:
        response = requests.get(url)
        console.rule("[bold blue]Fetching Subnet Analysis")
        console.print(f"Status Code: [bold green]{response.status_code}[/bold green]")
        data = response.json()
        analysis = data.get("analysis")
        display_analysis(analysis)
    except Exception as e:
        console.print(f"[bold red]Error fetching analysis:[/bold red] {e}")


def main():
    while True:
        log_details()
        time.sleep(60)


if __name__ == "__main__":
    main()
