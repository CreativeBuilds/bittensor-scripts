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
    Positive -> green, Negative -> red, Zero -> white.
    """
    try:
        val = float(delta)
    except (TypeError, ValueError):
        return str(delta)
    if val > 0:
        return f"[green]{val:.6f}[/green]"
    elif val < 0:
        return f"[red]{val:.6f}[/red]"
    else:
        return f"{val:.6f}"

def interpret_trend(final_gap, delta_gap):
    """
    Produce a text label indicating whether the gap is diverging or converging,
    and in which direction.

    Logic:
      - final_gap > 0 => Up; final_gap < 0 => Down.
      - delta_gap > 0 => Diverging; delta_gap < 0 => Converging.
      
    Combined:
      Diverging + Up    => [green]"Bullish"[/green]
      Diverging + Down  => [red]"Bearish"[/red]
      Converging + Up   => [red]"Pull Back"[/red]
      Converging + Down => [green]"Reversal"[/green]
      Otherwise         => "Neutral"
    """
    try:
        fg = float(final_gap)
        dg = float(delta_gap)
    except (TypeError, ValueError):
        return "[white]N/A[/white]"
    
    if abs(fg) < 1e-12 or abs(dg) < 1e-12:
        return "[white]Neutral[/white]"
    
    direction = "up" if fg > 0 else "down"
    divergence = "diverging" if dg > 0 else "converging"
    
    if divergence == "diverging" and direction == "up":
        return "[green]Bullish[/green]"
    elif divergence == "diverging" and direction == "down":
        return "[red]Bearish[/red]"
    elif divergence == "converging" and direction == "up":
        return "[red]Pull Back[/red]"
    elif divergence == "converging" and direction == "down":
        return "[green]Reversal[/green]"
    else:
        return "[white]Neutral[/white]"

def display_analysis(analysis):
    if not analysis:
        console.print("[bold red]No analysis data available.[/bold red]")
        return

    # Global Total Price Panel
    total_price = analysis.get("total_price", "N/A")
    total_price_ema = analysis.get("total_price_ema", "N/A")
    try:
        total_price_val = float(total_price)
        total_price_ema_val = float(total_price_ema)
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
    
    final_gap_total_price = analysis.get("final_gap_total_price")
    delta_gap_total_price = analysis.get("delta_gap_total_price")
    if final_gap_total_price is not None and delta_gap_total_price is not None:
        try:
            final_gap_str = f"{float(final_gap_total_price):.6f}"
        except (TypeError, ValueError):
            final_gap_str = str(final_gap_total_price)
        delta_gap_str = color_numeric_delta(delta_gap_total_price)
        global_trend = interpret_trend(final_gap_total_price, delta_gap_total_price)
        panel_text += (
            f"\n\n[bold cyan]Global Price Gap (5m vs 60m):[/bold cyan] {final_gap_str}"
            f"\n[bold cyan]Global Gap Δ(5m):[/bold cyan] {delta_gap_str}"
            f"\n[bold cyan]Global Trend:[/bold cyan] {global_trend}"
        )
    console.print(Panel(panel_text, title="Global Analysis", style="bold blue", expand=True))
    
    # Subnet-level analysis
    subnet_trends = analysis.get("subnet_gap_trends", [])
    if not subnet_trends:
        console.print("[bold red]No subnet gap trend analysis available.[/bold red]")
        return
    # Sort by current_emission descending
    subnet_trends_sorted = sorted(subnet_trends, key=lambda x: float(x.get("current_emission", 0)), reverse=True)
    
    # ---- Price Trends Table ----
    price_table = Table(title="Price Trends (Top 10)", box=box.DOUBLE_EDGE, expand=True)
    price_table.add_column("Netuid", justify="right")
    price_table.add_column("Current Price", justify="right")
    price_table.add_column("EMA5 Price", justify="right")
    price_table.add_column("EMA60 Price", justify="right")
    price_table.add_column("Gap Price", justify="right")
    price_table.add_column("Gap Price Δ(5m)", justify="right")
    price_table.add_column("Price Trend", justify="center")
    
    for trend in subnet_trends_sorted:
        final_gap_price = trend.get("final_gap_price", 0)
        delta_gap_price = trend.get("delta_gap_price", 0)
        price_delta_str = color_numeric_delta(delta_gap_price)
        price_trend_str = interpret_trend(final_gap_price, delta_gap_price)
        # Assume that the server sends a "current_price" field per subnet trend.
        current_price = trend.get("current_price", "N/A")
        price_table.add_row(
            str(trend.get("netuid", "N/A")),
            f"{current_price}",
            f'{trend.get("final_ema5_price", "N/A")}',
            f'{trend.get("final_ema60_price", "N/A")}',
            f'{trend.get("final_gap_price", "N/A")}',
            price_delta_str,
            price_trend_str,
        )
    console.print(price_table)
    
    # ---- Emission Trends Table ----
    emiss_table = Table(title="Emission Trends (Top 10)", box=box.DOUBLE_EDGE, expand=True)
    emiss_table.add_column("Netuid", justify="right")
    emiss_table.add_column("Current Emission", justify="right")
    emiss_table.add_column("EMA5 Emission", justify="right")
    emiss_table.add_column("EMA60 Emission", justify="right")
    emiss_table.add_column("Gap Emission", justify="right")
    emiss_table.add_column("Gap Emission Δ(5m)", justify="right")
    emiss_table.add_column("Emission Trend", justify="center")
    
    for trend in subnet_trends_sorted:
        final_gap_emission = trend.get("final_gap_emission", 0)
        delta_gap_emission = trend.get("delta_gap_emission", 0)
        emiss_delta_str = color_numeric_delta(delta_gap_emission)
        emiss_trend_str = interpret_trend(final_gap_emission, delta_gap_emission)
        emiss_table.add_row(
            str(trend.get("netuid", "N/A")),
            f'{trend.get("current_emission", "N/A")}',
            f'{trend.get("final_ema5_emission", "N/A")}',
            f'{trend.get("final_ema60_emission", "N/A")}',
            f'{trend.get("final_gap_emission", "N/A")}',
            emiss_delta_str,
            emiss_trend_str,
        )
    console.print(emiss_table)

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
        os.system('cls' if os.name == 'nt' else 'clear')
        log_details()
        time.sleep(60)

if __name__ == "__main__":
    main()
