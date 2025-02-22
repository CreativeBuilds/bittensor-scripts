import requests
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

BASE_URL = "http://localhost:7272"
console = Console()


def display_snapshot(snapshot, title="Snapshot"):
    table = Table(title=title, box=box.HEAVY_EDGE, show_lines=True)
    table.add_column("Field", style="bold cyan")
    table.add_column("Value", style="bold white")

    # Assuming snapshot has snapshot_id and snapshot_timestamp and records.
    table.add_row("snapshot_id", str(snapshot.get("snapshot_id", "N/A")))
    table.add_row("snapshot_timestamp", str(snapshot.get("snapshot_timestamp", "N/A")))
    console.print(table)

    records = snapshot.get("records", [])
    if records:
        rec_table = Table(title="Subnet Records", box=box.MINIMAL_DOUBLE_HEAD)
        rec_table.add_column("Netuid", style="magenta")
        rec_table.add_column("Price", style="green")
        rec_table.add_column("Emission", style="yellow")
        for rec in records:
            rec_table.add_row(
                str(rec.get("netuid", "N/A")),
                f'{rec.get("price", "N/A")}',
                f'{rec.get("emission", "N/A")}',
            )
        console.print(rec_table)


def display_subnets_response(data):
    # data is expected to be a dict with keys "records" and "analysis"
    records = data.get("records", [])
    analysis = data.get("analysis", {})

    # Display the raw records in a table
    if records:
        rec_table = Table(title="Subnet Records", box=box.SIMPLE_HEAVY)
        rec_table.add_column("Netuid", style="magenta", justify="right")
        rec_table.add_column("Price", style="green", justify="right")
        rec_table.add_column("Emission", style="yellow", justify="right")
        rec_table.add_column("Snapshot Timestamp", style="cyan", justify="center")
        for rec in records:
            rec_table.add_row(
                str(rec.get("netuid", "N/A")),
                str(rec.get("price", "N/A")),
                str(rec.get("emission", "N/A")),
                str(rec.get("snapshot_timestamp", "N/A")),
            )
        console.print(rec_table)
    else:
        console.print("[bold red]No subnet records found.[/bold red]")

    # Display analysis if available
    if analysis:
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
    else:
        console.print("[bold red]No analysis data available.[/bold red]")


def test_latest_snapshot():
    url = f"{BASE_URL}/snapshots/latest"
    try:
        response = requests.get(url)
        console.rule("[bold blue]Testing /snapshots/latest")
        console.print(f"Status Code: [bold green]{response.status_code}[/bold green]")
        snapshot = response.json()
        display_snapshot(snapshot, title="Latest Snapshot")
    except Exception as e:
        console.print(f"[bold red]Error testing /snapshots/latest:[/bold red] {e}")


def test_snapshot_by_id(snapshot_id):
    url = f"{BASE_URL}/snapshots/{snapshot_id}"
    try:
        response = requests.get(url)
        console.rule(f"[bold blue]Testing /snapshots/{snapshot_id}")
        console.print(f"Status Code: [bold green]{response.status_code}[/bold green]")
        snapshot = response.json()
        display_snapshot(snapshot, title=f"Snapshot {snapshot_id}")
    except Exception as e:
        console.print(f"[bold red]Error testing /snapshots/{snapshot_id}:[/bold red] {e}")


def test_subnets(filters=None):
    url = f"{BASE_URL}/subnets"
    try:
        params = filters if filters is not None else {}
        response = requests.get(url, params=params)
        console.rule("[bold blue]Testing /subnets")
        if filters:
            console.print(f"Filters: [bold yellow]{filters}[/bold yellow]")
        console.print(f"Status Code: [bold green]{response.status_code}[/bold green]")
        data = response.json()
        display_subnets_response(data)
    except Exception as e:
        console.print(f"[bold red]Error testing /subnets:[/bold red] {e}")


def test_rate_limit():
    url = f"{BASE_URL}/subnets"
    console.rule("[bold blue]Testing Rate Limiting Cache")
    try:
        # First request - fresh data
        response1 = requests.get(url)
        data1 = response1.json()
        console.print("[bold green]First response (fresh):[/bold green]")
        display_subnets_response(data1)
        
        time.sleep(5)
        
        # Second request - should be served from cache
        response2 = requests.get(url)
        data2 = response2.json()
        console.print("[bold green]Second response (cached):[/bold green]")
        display_subnets_response(data2)
        
        if data1 == data2:
            console.print("[bold green]Cache working as expected: responses are identical.[/bold green]")
        else:
            console.print("[bold red]Cache may not be working as expected: responses differ.[/bold red]")
    except Exception as e:
        console.print(f"[bold red]Error testing rate limit:[/bold red] {e}")


if __name__ == "__main__":
    test_latest_snapshot()
    test_snapshot_by_id(1)
    test_subnets()
    test_subnets(filters={"netuid": 1})
    test_rate_limit()
