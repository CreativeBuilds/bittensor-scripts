import os
import requests
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

BASE_URL = "http://localhost:7272"
console = Console(force_terminal=True, color_system="standard", no_color=False)

def color_numeric_delta(delta):
    """
    Return a string with the numeric delta color-coded:
      > 0 => green, < 0 => red, == 0 => white
    """
    try:
        val = float(delta)
    except (TypeError, ValueError):
        return str(delta)  # non-numeric or missing
    if val > 0:
        return f"[green]{val:.6f}[/green]"
    elif val < 0:
        return f"[red]{val:.6f}[/red]"
    else:
        return f"{val:.6f}"

def interpret_trend(final_gap, delta_gap):
    """
    Produce a text label indicating whether the final gap is
    'up' or 'down', and whether it's 'diverging' or 'converging'.

    Logic:
      final_gap > 0 => "Up"
      final_gap < 0 => "Down"
      delta_gap > 0 => "Diverging"
      delta_gap < 0 => "Converging"

    Combined:
      Diverging + Up => [green]"Bullish"[/green]
      Diverging + Down => [red]"Bearish"[/red]
      Converging + Up => [green]"Pull Back"[/green]
      Converging + Down => [red]"Reversal"[/red]
      If final_gap == 0 or delta_gap == 0 => "Neutral"
    """
    try:
        fg = float(final_gap)
        dg = float(delta_gap)
    except (TypeError, ValueError):
        return "[white]N/A[/white]"

    # If either is zero, label "Neutral"
    if abs(fg) < 1e-12 or abs(dg) < 1e-12:
        return "[white]Neutral[/white]"

    direction = "up" if fg > 0 else "down"
    divergence = "diverging" if dg > 0 else "converging"

    if divergence == "diverging" and direction == "up":
        return "[green]Bullish[/green]"
    elif divergence == "diverging" and direction == "down":
        return "[red]Bearish[/red]"
    elif divergence == "converging" and direction == "up":
        return "[green]Pull Back[/green]"
    elif divergence == "converging" and direction == "down":
        return "[red]Reversal[/red]"
    else:
        return "[white]Neutral[/white]"  # fallback

def display_analysis(analysis):
    """
    Display the global analysis (total price, total price EMA),
    plus a table of the top subnets with numeric deltas and textual trends.
    """
    if not analysis:
        console.print("[bold red]No analysis data available.[/bold red]")
        return

    # ---- Basic total price vs. total price EMA (color-coded) ----
    total_price = analysis.get("total_price", "N/A")
    total_price_ema = analysis.get("total_price_ema", "N/A")
    try:
        total_price_val = float(total_price)
        total_price_ema_val = float(total_price_ema)
        # If total price is above EMA, color green; else red
        if total_price_val > total_price_ema_val:
            total_price_str = f"[green]{total_price_val:.6f}[/green]"
        else:
            total_price_str = f"[red]{total_price_val:.6f}[/red]"
        total_price_ema_str = f"{total_price_ema_val:.6f}"
    except Exception:
        total_price_str = str(total_price)
        total_price_ema_str = str(total_price_ema)

    panel_text = (
        f"[bold cyan]Total Price:[/bold cyan] {total_price_str}\n"
        f"[bold cyan]Total Price EMA:[/bold cyan] {total_price_ema_str}"
    )
    console.print(Panel(panel_text, title="Global Analysis", style="bold blue"))

    # ---- Subnet-level 5m vs 60m gap trends (Top 10) ----
    subnet_trends = analysis.get("subnet_gap_trends", [])
    if subnet_trends:
        trend_table = Table(title="Subnet EMA Gap Trends (Top 10)", box=box.DOUBLE_EDGE)
        trend_table.add_column("Netuid", justify="right")
        trend_table.add_column("Current Emission", justify="right")
        trend_table.add_column("EMA5 Price", justify="right")
        trend_table.add_column("EMA60 Price", justify="right")
        trend_table.add_column("Gap Price", justify="right")
        trend_table.add_column("Gap Price Δ(5m)", justify="right")
        trend_table.add_column("Price Trend", justify="center")
        trend_table.add_column("EMA5 Emission", justify="right")
        trend_table.add_column("EMA60 Emission", justify="right")
        trend_table.add_column("Gap Emission", justify="right")
        trend_table.add_column("Gap Emission Δ(5m)", justify="right")
        trend_table.add_column("Emission Trend", justify="center")

        for trend in subnet_trends:
            final_gap_price = trend.get("final_gap_price", 0)
            delta_gap_price = trend.get("delta_gap_price", 0)
            final_gap_emission = trend.get("final_gap_emission", 0)
            delta_gap_emission = trend.get("delta_gap_emission", 0)

            # Numeric, color-coded deltas
            price_delta_str = color_numeric_delta(delta_gap_price)
            emiss_delta_str = color_numeric_delta(delta_gap_emission)

            # Textual trends
            price_trend_str = interpret_trend(final_gap_price, delta_gap_price)
            emiss_trend_str = interpret_trend(final_gap_emission, delta_gap_emission)

            trend_table.add_row(
                str(trend.get("netuid", "N/A")),
                f'{trend.get("current_emission", "N/A")}',
                f'{trend.get("final_ema5_price", "N/A")}',
                f'{trend.get("final_ema60_price", "N/A")}',
                f'{trend.get("final_gap_price", "N/A")}',
                price_delta_str,
                price_trend_str,
                f'{trend.get("final_ema5_emission", "N/A")}',
                f'{trend.get("final_ema60_emission", "N/A")}',
                f'{trend.get("final_gap_emission", "N/A")}',
                emiss_delta_str,
                emiss_trend_str,
            )
        console.print(trend_table)
    else:
        console.print("[bold red]No subnet gap trend analysis available.[/bold red]")


def log_details():
    """
    Fetch the /subnets endpoint, extract the analysis portion,
    and display it with numeric deltas and textual trends.
    """
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
        # Clear the terminal (works on Unix and Windows)
        os.system('cls' if os.name == 'nt' else 'clear')
        log_details()
        time.sleep(60)


if __name__ == "__main__":
    main()
