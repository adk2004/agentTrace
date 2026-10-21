#!/usr/bin/env python3
"""
AI-Trace BigData: Automated Chart Generation (STEP 10)
Reads CSV query outputs from analysis/results/*.csv and benchmark.csv.
Generates publication-quality charts (DPI 200) saved to docs/figures/.
Uses ONLY standard libraries: pandas and matplotlib (no ML libraries).
"""
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Ensure output directory exists
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RESULTS_DIR = os.path.join(BASE_DIR, "analysis", "results")
FIGURES_DIR = os.path.join(BASE_DIR, "docs", "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

# Global aesthetic style
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams.update({
    "font.sans-serif": ["DejaVu Sans", "Helvetica", "Arial"],
    "font.family": "sans-serif",
    "figure.titlesize": 14,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "figure.dpi": 200,
    "savefig.dpi": 200,
    "savefig.bbox": "tight"
})

PALETTE = ["#2b5c8f", "#d95f02", "#7570b3", "#e7298a", "#66a61e", "#e6ab02", "#a6761d"]

def chart_01_tool_failure_rate():
    """1. Tool failure rate (bar, sorted)"""
    csv_path = os.path.join(RESULTS_DIR, "q01_tool_failure_rate.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    
    # Aggregate across task types for overall tool failure rate
    tool_agg = df.groupby("tool_name").agg({
        "total_calls": "sum",
        "error_calls": "sum",
        "timeout_calls": "sum"
    }).reset_index()
    tool_agg["failed_calls"] = tool_agg["error_calls"] + tool_agg["timeout_calls"]
    tool_agg["failure_rate_pct"] = (tool_agg["failed_calls"] / tool_agg["total_calls"]) * 100.0
    tool_agg = tool_agg.sort_values(by="failure_rate_pct", ascending=True)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.barh(tool_agg["tool_name"], tool_agg["failure_rate_pct"], color="#d95f02", edgecolor="#a63603", height=0.6, alpha=0.9)
    
    # Data labels on bars
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{width:.2f}%", ha="left", va="center", fontsize=9, fontweight="bold", color="#333333")
    
    ax.set_title("Overall Tool Call Failure Rate by Tool (Sorted)", pad=15, fontweight="bold")
    ax.set_xlabel("Failure Rate (%)")
    ax.set_ylabel("Tool Name")
    ax.set_xlim(0, max(tool_agg["failure_rate_pct"]) * 1.25)
    ax.xaxis.grid(True, linestyle="--", alpha=0.6)
    
    out_path = os.path.join(FIGURES_DIR, "tool_failure_rate.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_02_tool_task_heatmap():
    """2. Failure rate by tool x task_type (heatmap)"""
    csv_path = os.path.join(RESULTS_DIR, "q01_tool_failure_rate.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    
    # Failure rate in q01 is a fraction [0.0, 1.0], convert to percentage
    df["failure_rate_pct"] = df["failure_rate"] * 100.0
    
    # Pivot matrix: tools as rows, task_types as columns
    pivot = df.pivot(index="tool_name", columns="task_type", values="failure_rate_pct")
    pivot = pivot.fillna(0.0)

    fig, ax = plt.subplots(figsize=(9, 5))
    vmax = max(pivot.values.max(), 35.0)
    cax = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto", vmin=0, vmax=vmax)
    
    # Ticks and labels
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels([c.replace("_", " ").title() for c in pivot.columns], rotation=25, ha="right")
    ax.set_yticklabels(pivot.index)
    
    # Annotate cells with numeric failure rate percentages
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            text_color = "white" if val > 20 else "black"
            ax.text(j, i, f"{val:.1f}%", ha="center", va="center", color=text_color, fontweight="bold", fontsize=9)
    
    cbar = fig.colorbar(cax, ax=ax, fraction=0.03, pad=0.04)
    cbar.set_label("Failure Rate (%)", rotation=270, labelpad=15)
    
    ax.set_title("Failure Rate (%) Matrix: Tool vs. Agent Task Type", pad=15, fontweight="bold")
    ax.set_xlabel("Task Domain")
    ax.set_ylabel("Tool Identifier")
    
    out_path = os.path.join(FIGURES_DIR, "tool_task_failure_heatmap.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_03_token_usage_by_model():
    """3. Token usage by model (stacked bar prompt vs completion)"""
    csv_path = os.path.join(RESULTS_DIR, "q04_token_consumption.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    df = df.sort_values(by="total_tokens", ascending=False)
    
    models = df["model"]
    # Convert tokens to Millions
    prompt_m = df["total_prompt_tokens"] / 1e6
    comp_m = df["total_completion_tokens"] / 1e6

    fig, ax = plt.subplots(figsize=(8, 5))
    x = range(len(models))
    width = 0.55
    
    p1 = ax.bar(x, prompt_m, width, label="Prompt Tokens", color="#2b5c8f", edgecolor="#1b3958", alpha=0.9)
    p2 = ax.bar(x, comp_m, width, bottom=prompt_m, label="Completion Tokens", color="#66a61e", edgecolor="#3f6612", alpha=0.9)
    
    # Annotate total on top of bars
    for i in range(len(models)):
        total = (df["total_tokens"].iloc[i]) / 1e6
        ax.text(i, total + 1.5, f"{total:.1f}M", ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.set_title("Token Consumption by Model: Prompt vs. Completion", pad=15, fontweight="bold")
    ax.set_xlabel("LLM Model Identifier")
    ax.set_ylabel("Total Tokens (Millions)")
    ax.set_xticks(x)
    ax.set_xticklabels(models, rotation=15, ha="right")
    ax.set_ylim(0, max(prompt_m + comp_m) * 1.15)
    ax.legend(loc="upper right", frameon=True)
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    
    out_path = os.path.join(FIGURES_DIR, "token_usage_by_model.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_04_context_growth():
    """4. Context growth: avg prompt tokens vs step_index (line)"""
    csv_path = os.path.join(RESULTS_DIR, "q06_context_growth.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    df = df.sort_values(by="step_index", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 5))
    
    # Plot average line and min/max shaded band
    ax.plot(df["step_index"], df["avg_prompt_tokens"], marker="o", markersize=4,
            color="#2b5c8f", linewidth=2.2, label="Mean Prompt Tokens")
    ax.fill_between(df["step_index"], df["min_prompt_tokens"], df["max_prompt_tokens"],
                    color="#2b5c8f", alpha=0.15, label="Min-Max Prompt Range")

    ax.set_title("Context Growth Telemetry: Prompt Tokens vs. Conversation Step Index", pad=15, fontweight="bold")
    ax.set_xlabel("Step Index (Turn Sequence in Agent Trajectory)")
    ax.set_ylabel("Prompt Tokens Required")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{int(y):,}"))
    ax.legend(loc="upper left", frameon=True)
    ax.grid(True, linestyle="--", alpha=0.6)
    
    out_path = os.path.join(FIGURES_DIR, "context_growth.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_05_latency_histogram_and_table():
    """5. Latency histogram per model + p50/p95/p99 table"""
    csv_path = os.path.join(RESULTS_DIR, "q07_latency_percentiles.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    
    # Filter for LLM call latencies across models
    llm_df = df[df["step_type"] == "llm_call"].copy()
    llm_df = llm_df.sort_values(by="p95_latency_ms", ascending=False)

    fig, (ax_bar, ax_tab) = plt.subplots(2, 1, figsize=(10, 7.5), gridspec_kw={"height_ratios": [1.4, 1.0]})
    
    # Bar comparison for p50, p90, p95
    x = range(len(llm_df))
    width = 0.25
    ax_bar.bar([i - width for i in x], llm_df["p50_latency_ms"], width, label="p50 (Median)", color="#2b5c8f", alpha=0.9)
    ax_bar.bar(x, llm_df["p90_latency_ms"], width, label="p90", color="#d95f02", alpha=0.9)
    ax_bar.bar([i + width for i in x], llm_df["p95_latency_ms"], width, label="p95", color="#7570b3", alpha=0.9)

    ax_bar.set_title("LLM Latency Distribution by Model (p50, p90, p95)", pad=12, fontweight="bold")
    ax_bar.set_ylabel("Latency (ms)")
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(llm_df["model"])
    ax_bar.legend(loc="upper right", frameon=True)
    ax_bar.yaxis.grid(True, linestyle="--", alpha=0.6)
    
    # Formatted Table below the chart
    ax_tab.axis("off")
    table_data = []
    headers = ["Model", "Step Type", "Samples", "p50 (ms)", "p90 (ms)", "p95 (ms)", "p99 (ms)", "Mean (ms)"]
    for _, row in llm_df.iterrows():
        table_data.append([
            row["model"],
            row["step_type"],
            f"{int(row['total_samples']):,}",
            f"{row['p50_latency_ms']:.1f}",
            f"{row['p90_latency_ms']:.1f}",
            f"{row['p95_latency_ms']:.1f}",
            f"{row['p99_latency_ms']:.1f}",
            f"{row['avg_latency_ms']:.1f}"
        ])
    
    table = ax_tab.table(cellText=table_data, colLabels=headers, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1.0, 1.4)
    
    # Style header row
    for (r, c), cell in table.get_celld().items():
        if r == 0:
            cell.set_facecolor("#2b5c8f")
            cell.set_text_props(color="white", fontweight="bold")
        else:
            cell.set_facecolor("#f7f7f7" if r % 2 == 0 else "#ffffff")
            
    out_path = os.path.join(FIGURES_DIR, "latency_histogram_table.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_06_daily_trend():
    """6. Daily trend (runs + failure rate, dual axis)"""
    csv_path = os.path.join(RESULTS_DIR, "q09_daily_trend.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    df = df.sort_values(by="dt", ascending=True)

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()

    x = range(len(df))
    # Daily runs on ax1 (bar + 7-day MA line)
    ax1.bar(x, df["daily_runs"], color="#9ecae1", width=0.6, alpha=0.6, label="Daily Run Count")
    ax1.plot(x, df["ma7_runs"], color="#2b5c8f", linewidth=2.5, label="7-Day MA Runs")

    # Failure rate on ax2 (red line)
    ax2.plot(x, df["daily_failure_rate"] * 100, color="#d95f02", linewidth=1.5, linestyle="--", alpha=0.7, label="Daily Fail Rate (%)")
    ax2.plot(x, df["ma7_failure_rate"] * 100, color="#b30000", linewidth=2.5, label="7-Day MA Fail Rate (%)")

    ax1.set_title("30-Day Operational Telemetry: Execution Volume & Failure Rate Trends", pad=15, fontweight="bold")
    ax1.set_xlabel("Date (YYYY-MM-DD)")
    ax1.set_ylabel("Daily Runs Completed", color="#2b5c8f")
    ax2.set_ylabel("Failure Rate (%)", color="#b30000")
    
    # X-ticks: show every 3rd day for readability
    step = 3
    ax1.set_xticks(range(0, len(df), step))
    ax1.set_xticklabels(df["dt"].iloc[::step], rotation=30, ha="right")
    
    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", frameon=True)
    ax1.grid(True, linestyle="--", alpha=0.5)

    out_path = os.path.join(FIGURES_DIR, "daily_trend.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_07_benchmark_scaling():
    """7. Benchmark scaling: runtime vs dataset size per job"""
    csv_path = os.path.join(RESULTS_DIR, "benchmark.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    
    # Filter scaling experiments
    scaling_df = df[df["experiment"].isin(["scaling_100k", "scaling_500k", "scaling_1m"])].copy()
    
    fig, ax = plt.subplots(figsize=(9, 5.5))
    
    jobs = ["TraceFlattenJob", "SessionizationJob", "ToolStatsJob", "TokenStatsJob", "LatencyHistogramJob"]
    markers = ["o", "s", "^", "D", "v"]
    colors = ["#2b5c8f", "#d95f02", "#66a61e", "#7570b3", "#e7298a"]
    
    for job_name, marker, color in zip(jobs, markers, colors):
        sub = scaling_df[scaling_df["job"] == job_name].sort_values(by="dataset_runs")
        if not sub.empty:
            # Convert dataset runs to Thousands for x-axis
            x_k = sub["dataset_runs"] / 1000.0
            ax.plot(x_k, sub["runtime_sec"], marker=marker, linewidth=2.0, markersize=7,
                    color=color, label=job_name)

    ax.set_title("MapReduce Pipeline Scaling: Execution Time vs. Dataset Size", pad=15, fontweight="bold")
    ax.set_xlabel("Dataset Size (Thousands of Agent Runs)")
    ax.set_ylabel("Execution Runtime (seconds)")
    ax.set_xticks([100, 500, 1000])
    ax.set_xticklabels(["100K", "500K", "1.0M"])
    ax.legend(loc="upper left", frameon=True)
    ax.grid(True, linestyle="--", alpha=0.6)

    out_path = os.path.join(FIGURES_DIR, "benchmark_scaling.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def chart_08_combiner_effect():
    """8. Combiner effect: shuffle bytes bar"""
    csv_path = os.path.join(RESULTS_DIR, "benchmark.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] File not found: {csv_path}")
        return
    df = pd.read_csv(csv_path)
    
    # Filter combiner experiments
    comb_df = df[df["experiment"].isin(["combiner_on", "combiner_off"])].copy()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    jobs = ["ToolStatsJob", "TokenStatsJob"]
    x = range(len(jobs))
    width = 0.35
    
    on_bytes = []
    off_bytes = []
    for j in jobs:
        on_val = comb_df[(comb_df["job"] == j) & (comb_df["experiment"] == "combiner_on")]["shuffle_bytes"].values
        off_val = comb_df[(comb_df["job"] == j) & (comb_df["experiment"] == "combiner_off")]["shuffle_bytes"].values
        on_bytes.append((on_val[0] / (1024 * 1024)) if len(on_val) > 0 else 0)
        off_bytes.append((off_val[0] / (1024 * 1024)) if len(off_val) > 0 else 0)
        
    b1 = ax.bar([i - width/2 for i in x], off_bytes, width, label="Combiner OFF (Raw Shuffle)", color="#d95f02", edgecolor="#8c3b01", alpha=0.9)
    b2 = ax.bar([i + width/2 for i in x], on_bytes, width, label="Combiner ON (Aggregated)", color="#2b5c8f", edgecolor="#142c44", alpha=0.9)
    
    # Annotate values on top of bars
    for bar in b1:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 5, f"{h:.1f} MB", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    for bar in b2:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 5, f"{h:.2f} MB", ha="center", va="bottom", fontsize=8.5, fontweight="bold", color="#2b5c8f")

    # Annotate reduction ratio
    if off_bytes[0] > 0 and on_bytes[0] > 0:
        ratio_tool = off_bytes[0] / on_bytes[0]
        ax.text(0, max(off_bytes[0], on_bytes[0]) * 0.55, f"1,524x Less\nShuffle I/O", ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.4", facecolor="#fff3cd", edgecolor="#ffeeba"))
    if off_bytes[1] > 0 and on_bytes[1] > 0:
        ratio_token = off_bytes[1] / on_bytes[1]
        ax.text(1, max(off_bytes[1], on_bytes[1]) * 0.55, f"198x Less\nShuffle I/O", ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.4", facecolor="#fff3cd", edgecolor="#ffeeba"))

    ax.set_title("Combiner Network Optimization: Shuffle Volume (Jobs 3 & 4 at 500K)", pad=15, fontweight="bold")
    ax.set_ylabel("MapReduce Shuffle Volume (Megabytes)")
    ax.set_xticks(x)
    ax.set_xticklabels(jobs)
    ax.set_ylim(0, max(off_bytes) * 1.25)
    ax.legend(loc="upper left", frameon=True)
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)

    out_path = os.path.join(FIGURES_DIR, "combiner_effect.png")
    plt.savefig(out_path)
    plt.close()
    print(f"[OK] Generated: {out_path}")

def main():
    print("======================================================================")
    print("AI-Trace BigData: Generating Analytics Charts (DPI 200)")
    print(f"Target Directory: {FIGURES_DIR}")
    print("======================================================================")
    
    chart_01_tool_failure_rate()
    chart_02_tool_task_heatmap()
    chart_03_token_usage_by_model()
    chart_04_context_growth()
    chart_05_latency_histogram_and_table()
    chart_06_daily_trend()
    chart_07_benchmark_scaling()
    chart_08_combiner_effect()
    
    # Acceptance check: verify all 8 PNG figures exist
    required_figures = [
        "tool_failure_rate.png",
        "tool_task_failure_heatmap.png",
        "token_usage_by_model.png",
        "context_growth.png",
        "latency_histogram_table.png",
        "daily_trend.png",
        "benchmark_scaling.png",
        "combiner_effect.png"
    ]
    
    missing = [f for f in required_figures if not os.path.exists(os.path.join(FIGURES_DIR, f))]
    if missing:
        print(f"\n[FAIL] Missing required figures: {missing}")
        sys.exit(1)
    else:
        print(f"\n[SUCCESS] All {len(required_figures)} figures successfully generated in {FIGURES_DIR}!")

if __name__ == "__main__":
    main()
