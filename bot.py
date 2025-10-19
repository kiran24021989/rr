import logging
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
import os
TOKEN = os.getenv("TELEGRAM_TOKEN")

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ------------------- Load Excel Data -------------------
EXCEL_ACT = "ACT.xlsx"
df_act = pd.read_excel(EXCEL_ACT, sheet_name="act")
df_tgt = pd.read_excel(EXCEL_ACT, sheet_name="tgt")
df_sch = pd.read_excel(EXCEL_ACT, sheet_name="act")
df_rts = pd.read_excel(EXCEL_ACT, sheet_name="rts")
EXCEL_DATA = "ACT.xlsx"
df_cache = pd.read_excel(EXCEL_DATA, sheet_name="ser")
df_cache.columns = df_cache.columns.str.strip()
df_cache["DATE"] = pd.to_datetime(df_cache["DATE"], errors="coerce")

# ------------------- Script3 prep -------------------
def get_financial_year(dt):
    if dt.month >= 4:
        return f"{dt.year}-{str(dt.year+1)[-2:]}"
    else:
        return f"{dt.year-1}-{str(dt.year)[-2:]}"
df_cache["FY"] = df_cache["DATE"].apply(get_financial_year)
df_cache["Month"] = df_cache["DATE"].dt.strftime("%b")

FILTER_ORDER = ["FY", "Month", "DEPOT", "Product", "ROUTE", "SER"]
FY_MONTH_ORDER = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]

# ------------------- Helper functions -------------------
def build_keyboard_matrix(items, n_cols=3, prefix="ITEM|"):
    buttons = [InlineKeyboardButton(str(item), callback_data=f"{prefix}{item}") for item in items]
    return InlineKeyboardMarkup([buttons[i:i+n_cols] for i in range(0, len(buttons), n_cols)])

# ------------------- ACT/TGT formatting functions -------------------
def format_sch_vs_fleet(df_filtered, depot=None, month=None, all_months=False):
    if df_filtered.empty:
        return "❌ No data found for given filters."

    # --- CLEAN & PREPARE DATA ---
    def clean_table(df):
        numeric_cols = [
            "CY HLD", "CY SCH", "CY SER", "CY SCH KMS",
            "LY HLD", "LY SCH", "LY SER", "LY SCH KMS",
            "VAR HLD", "VAR SCH", "VAR SER", "VAR SCH KMS"
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        cy_cols = ["CY HLD", "CY SCH", "CY SER", "CY SCH KMS"]
        df = df.loc[(df[cy_cols].sum(axis=1) != 0)]
        return df

    df_filtered = clean_table(df_filtered.copy())
    if df_filtered.empty:
        return "❌ No valid data after cleaning."

    # --- HEADER MAKERS ---
    def make_header(label_name):
        return (
            f"{label_name:<6}|     CY     |     LY     |    VAR\n"
            f"{'':<6}| FLT SCH SER| FLT SCH SER| FLT SCH SER\n"
            + "-" * 40 + "\n"
        )

    def make_header_kms(label_name):
        return (
            f"{label_name:<6}| CY KMS | LY KMS |  VAR\n"
            + "-" * 35 + "\n"
        )

    # --- ROW MAKERS ---
    def make_rows(df, label_col):
        rows = ""
        for _, r in df.iterrows():
            rows += (
                f"{r[label_col]:<6}| "
                f"{r['CY HLD']:>3} {r['CY SCH']:>3} {r['CY SER']:>3}| "
                f"{r['LY HLD']:>3} {r['LY SCH']:>3} {r['LY SER']:>3}| "
                f"{r['VAR HLD']:>3} {r['VAR SCH']:>3} {r['VAR SER']:>3}\n"
            )
        return rows

    def make_rows_kms(df, label_col):
        rows = ""
        for _, r in df.iterrows():
            rows += (
                f"{r[label_col]:<6}| "
                f"{r['CY SCH KMS']:>6} | "
                f"{r['LY SCH KMS']:>6} | "
                f"{r['VAR SCH KMS']:>6}\n"
            )
        return rows

    tables = []

    # --- CASE 1: MONTH SELECTED → DEPOT-WISE DATA ---
    if month:
        title_line = f"📊 Fleet, SCH & SER Summary (Depot-wise) for {month}"
        df_month = df_filtered[df_filtered["MONTH"].str.upper() == month.upper()]
        if df_month.empty:
            return f"❌ No data found for month {month}."

        label = "DEPOT"
        tables.append(f"```\n{make_header(label)}{make_rows(df_month, label)}```")
        tables.append(f"🏁 Schedule KMs\n```\n{make_header_kms(label)}{make_rows_kms(df_month, label)}```")

    # --- CASE 2: DEPOT SELECTED → MONTH-WISE DATA ---
    elif depot:
        title_line = f"📊 Fleet, SCH & SER Summary of {depot} (Month-wise)"
        df_depot = df_filtered[df_filtered["DEPOT"].str.upper() == depot.upper()]
        if df_depot.empty:
            return f"❌ No data found for depot {depot}."

        label = "MONTH"
        tables.append(f"```\n{make_header(label)}{make_rows(df_depot, label)}```")
        tables.append(f"🏁 Schedule KMs\n```\n{make_header_kms(label)}{make_rows_kms(df_depot, label)}```")

    # --- CASE 3: ALL DEPOTS, ALL MONTHS ---
    else:
        title_line = f"📊 HLD & SCH SUMMARY (All Depots – All Months)"
        for depot_name, df_depot in df_filtered.groupby("DEPOT"):
            tables.append(f"🏢 {depot_name}\n```\n{make_header('MONTH')}{make_rows(df_depot, 'MONTH')}```")
            tables.append(f"🏢 Schedule KMs\n```\n{make_header_kms('MONTH')}{make_rows_kms(df_depot, 'MONTH')}```")

    return f"*SCH vs FLEET REPORT*\n{title_line}\n" + "\n".join(tables)


def format_act_vs_act(df_filtered, depot=None, month=None, all_months=False):
    if df_filtered.empty:
        return "❌ No data found for given filters."
    def clean_table(df, col_prefix):
        df_clean = df[~((df[f"CY {col_prefix}"]==0) & (df[f"LY {col_prefix}"]==0) & (df[f"VAR {col_prefix}"]==0))].copy()
        for c in [f"CY {col_prefix}", f"LY {col_prefix}", f"VAR {col_prefix}"]:
            df_clean[c] = df_clean[c].round(2) if col_prefix not in ["AVU","EPB"] else df_clean[c].round(0).astype(int)
        return df_clean

    metrics = ["KMS","EARN","EPK","OR","AVU","EPB"]
    tables = []
    for m in metrics:
        df_m = clean_table(df_filtered.copy(), m)
        if df_m.empty:
            tables.append(f"❌ No {m} data.")
            continue

        # Month vs Depot view
        if month:  # Month selected → Show all depots
            header = f"{'DEPOT':<6}{'MONTH':<5}{'CY':>6} {'LY':>8}  {'VAR':>6} {'%ACH':>6}\n" + "-"*40 + "\n"
            rows = ""
            for _, r in df_m.iterrows():
                cy, ly, var, ach = r[f"CY {m}"], r[f"LY {m}"], r[f"VAR {m}"], r[f"% OF ACH {m}"]
                cy, ly, var = [f"{x:>8.0f}" if m in ["AVU","EPB"] else f"{x:>8.2f}" for x in [cy, ly, var]]
                ach = f"{ach:>8.2f}"
                rows += f"{r['DEPOT']:<6}{r['MONTH']:<6}{cy}{ly}{var}{ach}\n"
        else:  # Depot selected → Show monthly trend
            header = f"{'MONTH':<5}{'CY':>6} {'LY':>8}  {'VAR':>6} {'%ACH':>6}\n" + "-"*35 + "\n"
            rows = ""
            for _, r in df_m.iterrows():
                cy, ly, var, ach = r[f"CY {m}"], r[f"LY {m}"], r[f"VAR {m}"], r[f"% OF ACH {m}"]
                cy, ly, var = [f"{x:>8.0f}" if m in ["AVU","EPB"] else f"{x:>8.2f}" for x in [cy, ly, var]]
                ach = f"{ach:>8.2f}"
                rows += f"{r['MONTH']:<5}{cy}{ly}{var}{ach}\n"

        title_line = f"📊 {m} Table"
        if depot and not month:
            title_line += f" ({depot} – All Months)"
        elif month and not depot:
            title_line += f" (All Depots – {month})"

        tables.append(f"{title_line}\n```\n{header}{rows}```")
    return "*ACT vs ACT REPORT*\n" + "\n".join(tables)


def get_filter_options(df):
    def to_str_list(col):
        return ["All"] + sorted(df[col].dropna().astype(str).unique().tolist())

    return {
        "Month": to_str_list("Month") if "Month" in df.columns else ["All"],
        "DEPOT": to_str_list("DEPOT") if "DEPOT" in df.columns else ["All"],
        "Product": to_str_list("Product") if "Product" in df.columns else ["All"],
        "ROUTE": to_str_list("ROUTE") if "ROUTE" in df.columns else ["All"],
    }
def format_route_sch(df_filtered, filters):
    """
    Filters df_filtered based on filters dict and formats a nicely aligned route schedule
    for Telegram messages. Handles 'All', single values, and lists.
    """
    if df_filtered.empty:
        return "❌ No data available."

    # Apply filters safely
    for col, val in filters.items():
        if col not in df_filtered.columns:
            continue
        if val is None or val == "All":
            continue
        if isinstance(val, list):
            if len(val) == 0:
                continue
            df_filtered = df_filtered[df_filtered[col].isin(val)]
        else:
            # Convert single value to list for uniform handling
            df_filtered = df_filtered[df_filtered[col].isin([val])]

    if df_filtered.empty:
        return "❌ No data found for selection."

    # === Format message with aligned columns ===
    table_lines = []
    subtotal_cols = ["NO SCH", "NO SER", "SCH KMS"]

    for depot, depot_df in df_filtered.groupby("DEPOT", sort=False):
        table_lines.append(f"*Depot: {depot}*")
        table_lines.append("Month | NO SCH | NO SER | SCH KMS | Product | Route")
        for _, row in depot_df.iterrows():
            table_lines.append(
                f"{row['Month']} | {row['NO SCH']} | {row['NO SER']} | {row['SCH KMS']} | {row['Product']} | {row['ROUTE']}"
            )
        subtotal = depot_df[subtotal_cols].sum()
        table_lines.append(
            f"*Subtotal {depot}: {subtotal['NO SCH']} | {subtotal['NO SER']} | {subtotal['SCH KMS']}*\n"
        )

    grand_total = df_filtered[subtotal_cols].sum()
    table_lines.append(
        f"*Grand Total: {grand_total['NO SCH']} | {grand_total['NO SER']} | {grand_total['SCH KMS']}*"
    )

    return "\n".join(table_lines)


def format_tgt_vs_act(df_filtered, depot=None, month=None, all_months=False):
    if df_filtered.empty:
        return "❌ No data found for given filters."
    def clean_table(df, col_prefix):
        df_clean = df[~((df[f"TGT {col_prefix}"]==0) & (df[f"ACT {col_prefix}"]==0) & (df[f"VAR {col_prefix}"]==0))].copy()
        for c in [f"TGT {col_prefix}", f"ACT {col_prefix}", f"VAR {col_prefix}"]:
            df_clean[c] = df_clean[c].round(2) if col_prefix not in ["AVU","EPB"] else df_clean[c].round(0).astype(int)
        return df_clean

    metrics = ["KMS","EARN","EPK","OR","AVU","EPB"]
    tables = []
    for m in metrics:
        df_m = clean_table(df_filtered.copy(), m)
        if df_m.empty:
            tables.append(f"❌ No {m} data.")
            continue

        # Month vs Depot view
        if month:  # Month selected → Show all depots
            header = f"{'DEPOT':<5}{'MONTH':<5}{'TGT':>6} {'ACT':>8} {'VAR':>6} {'%ACH':>6}\n" + "-"*40 + "\n"
            rows = ""
            for _, r in df_m.iterrows():
                tgt, act, var, ach = r[f"TGT {m}"], r[f"ACT {m}"], r[f"VAR {m}"], r[f"% OF ACH {m}"]
                tgt, act, var = [f"{x:>8.0f}" if m in ["AVU","EPB"] else f"{x:>8.2f}" for x in [tgt, act, var]]
                ach = f"{ach:>8.2f}"
                rows += f"{r['DEPOT']:<5}{r['MONTH']:<5}{tgt}{act}{var}{ach}\n"
        else:  # Depot selected → Show monthly trend
            header = f"{'MONTH':<5}{'TGT':>6} {'ACT':>8} {'VAR':>6} {'%ACH':>6}\n" + "-"*35 + "\n"
            rows = ""
            for _, r in df_m.iterrows():
                tgt, act, var, ach = r[f"TGT {m}"], r[f"ACT {m}"], r[f"VAR {m}"], r[f"% OF ACH {m}"]
                tgt, act, var = [f"{x:>8.0f}" if m in ["AVU","EPB"] else f"{x:>8.2f}" for x in [tgt, act, var]]
                ach = f"{ach:>8.2f}"
                rows += f"{r['MONTH']:<5}{tgt}{act}{var}{ach}\n"

        title_line = f"📊 {m} Table"
        if depot and not month:
            title_line += f" ({depot} – All Months)"
        elif month and not depot:
            title_line += f" (All Depots – {month})"

        tables.append(f"{title_line}\n```\n{header}{rows}```")

    return "*TGT vs ACT REPORT*\n" + "\n".join(tables)
def monthwise_kms_pass_table(df_cur, df_prev, month_order=None, show_cumulative=True, group_col='Month'):
    """Monthwise or Depotwise KMs & Pass Comparison table."""

    if month_order is None:
        month_order = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]

    # --- Aggregate current and previous data ---
    cur = df_cur.groupby(group_col).agg({
        'TOT PASS': 'sum',
        'KMS': 'sum'
    }).reset_index()

    prev = df_prev.groupby(group_col).agg({
        'TOT PASS': 'sum',
        'KMS': 'sum'
    }).reset_index()

    merged = pd.merge(cur, prev, on=group_col, how='outer', suffixes=('_cur', '_prev')).fillna(0)

    # --- Sorting ---
    if group_col == 'Month':
        merged[group_col] = pd.Categorical(merged[group_col], categories=month_order, ordered=True)
        merged = merged.sort_values(group_col)
    else:
        merged = merged.sort_values(group_col)

    # --- Skip if no data ---
    if merged[['KMS_cur', 'TOT PASS_cur']].sum().sum() == 0 and \
       merged[['KMS_prev', 'TOT PASS_prev']].sum().sum() == 0:
        return ""

    # --- Table formatting ---
    header_label = "Month" if group_col == "Month" else "Depot"
    lines = []
    header = f"{header_label:<4} {'KMs':>9} {'        |    Psngrs.':>10} "
    subheader = f"{'':<4} {'| cy  , ly  ,  var |':>18} {' cy , ly  , var':>12} "
    underline = "-" * len(header)
    lines.extend([header, subheader, underline])

    # --- Row-wise EPK calculations ---
    for _, row in merged.iterrows():
        kms_cur = row['KMS_cur']/100000
        kms_prev = row['KMS_prev']/100000
        pass_cur   = row['TOT PASS_cur']/100000
        pass_prev  = row['TOT PASS_prev']/100000
        var_kms       = kms_cur - kms_prev
        var_pass     = pass_cur - pass_prev

        lines.append(
            f"{str(row[group_col]):<4}"
            f" |{kms_cur:.2f} , {kms_prev:.2f}, {var_kms:+.2f}| "
            f"{pass_cur:.2f},{pass_prev:.2f} ,{var_pass:+.2f}"
            
        )

    # --- Cumulative (CUM) row ---
    if show_cumulative:
        tot_kms_cur  = merged['KMS_cur'].sum()/100000 
        tot_kms_prev = merged['KMS_prev'].sum()/100000 
        tot_pass_cur   = merged['TOT PASS_cur'].sum() /100000
        tot_pass_prev = merged['TOT PASS_prev'].sum() / 100000
        tot_var_kms       = tot_kms_cur - tot_kms_prev
        tot_var_pass     = tot_pass_cur - tot_pass_prev

        lines.append(underline)
        lines.append(
            f"{'CUM':<4}"
            f" |{tot_kms_cur:.2f},{tot_kms_prev:.2f}, {tot_var_kms:+.2f}|"
            f" {tot_pass_cur:.2f},{tot_pass_prev:.2f}, {tot_var_pass:+.2f}"
        )
        lines.append(underline)

    return "\n".join(lines)


def monthwise_net_gross_epk_table(df_cur, df_prev, month_order=None, show_cumulative=True, group_col='Month'):
    """Monthwise or Depotwise NET & GROSS EPK comparison table."""

    if month_order is None:
        month_order = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]

    # --- Aggregate current and previous data ---
    cur = df_cur.groupby(group_col).agg({
        'NET (E)': 'sum',
        'GROSS (E)': 'sum',
        'KMS': 'sum'
    }).reset_index()

    prev = df_prev.groupby(group_col).agg({
        'NET (E)': 'sum',
        'GROSS (E)': 'sum',
        'KMS': 'sum'
    }).reset_index()

    merged = pd.merge(cur, prev, on=group_col, how='outer', suffixes=('_cur', '_prev')).fillna(0)

    # --- Sorting ---
    if group_col == 'Month':
        merged[group_col] = pd.Categorical(merged[group_col], categories=month_order, ordered=True)
        merged = merged.sort_values(group_col)
    else:
        merged = merged.sort_values(group_col)

    # --- Skip if no data ---
    if merged[['NET (E)_cur', 'GROSS (E)_cur']].sum().sum() == 0 and \
       merged[['NET (E)_prev', 'GROSS (E)_prev']].sum().sum() == 0:
        return ""

    # --- Table formatting ---
    header_label = "Month" if group_col == "Month" else "Depot"
    lines = []
    header = f"{header_label:<4} {'CY':>6} {'LY':>12} {'Var':>12}"
    subheader = f"{'':<4} {'| Net , Grs |':>10} {' Net , Grs |':>10} {'Net , Grs':>10}"
    underline = "-" * len(header)
    lines.extend([header, subheader, underline])

    # --- Row-wise EPK calculations ---
    for _, row in merged.iterrows():
        kms_cur = row['KMS_cur']
        kms_prev = row['KMS_prev']
        net_epk_cur   = row['NET (E)_cur'] / kms_cur if kms_cur else 0
        gross_epk_cur = row['GROSS (E)_cur'] / kms_cur if kms_cur else 0
        net_epk_prev  = row['NET (E)_prev'] / kms_prev if kms_prev else 0
        gross_epk_prev= row['GROSS (E)_prev'] / kms_prev if kms_prev else 0
        var_net       = net_epk_cur - net_epk_prev
        var_gross     = gross_epk_cur - gross_epk_prev

        lines.append(
            f"{str(row[group_col]):<4}"
            f" |{net_epk_cur:.2f},{gross_epk_cur:.2f}|"
            f" {net_epk_prev:.2f},{gross_epk_prev:.2f}|"
            f" {var_net:+.2f},{var_gross:+.2f}"
        )

    # --- Cumulative (CUM) row ---
    if show_cumulative:
        tot_kms_cur  = merged['KMS_cur'].sum()
        tot_kms_prev = merged['KMS_prev'].sum()
        tot_net_epk_cur   = merged['NET (E)_cur'].sum() / tot_kms_cur if tot_kms_cur else 0
        tot_gross_epk_cur = merged['GROSS (E)_cur'].sum() / tot_kms_cur if tot_kms_cur else 0
        tot_net_epk_prev  = merged['NET (E)_prev'].sum() / tot_kms_prev if tot_kms_prev else 0
        tot_gross_epk_prev= merged['GROSS (E)_prev'].sum() / tot_kms_prev if tot_kms_prev else 0
        tot_var_net       = tot_net_epk_cur - tot_net_epk_prev
        tot_var_gross     = tot_gross_epk_cur - tot_gross_epk_prev

        lines.append(underline)
        lines.append(
            f"{'CUM':<4}"
            f" |{tot_net_epk_cur:.2f},{tot_gross_epk_cur:.2f}|"
            f" {tot_net_epk_prev:.2f},{tot_gross_epk_prev:.2f}|"
            f" {tot_var_net:+.2f},{tot_var_gross:+.2f}"
        )
        lines.append(underline)

    return "\n".join(lines)


def monthwise_net_gross_mhl_epk_table(df_cur, df_prev, month_order=None, show_cumulative=True, group_col='Month'):
    """Monthwise or Depotwise MHL NET & GROSS EPK comparison table."""

    if month_order is None:
        month_order = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]

    # --- Aggregate current and previous data ---
    cur = df_cur.groupby(group_col).agg({
        'MHL NET(E)': 'sum',
        'MHL GROSS(E)': 'sum',
        'KMS': 'sum'
    }).reset_index()

    prev = df_prev.groupby(group_col).agg({
        'MHL NET(E)': 'sum',
        'MHL GROSS(E)': 'sum',
        'KMS': 'sum'
    }).reset_index()

    merged = pd.merge(cur, prev, on=group_col, how='outer', suffixes=('_cur', '_prev')).fillna(0)

    # --- Sorting ---
    if group_col == 'Month':
        merged[group_col] = pd.Categorical(merged[group_col], categories=month_order, ordered=True)
        merged = merged.sort_values(group_col)
    else:
        merged = merged.sort_values(group_col)

    # --- Skip if no data ---
    if merged[['MHL NET(E)_cur', 'MHL GROSS(E)_cur']].sum().sum() == 0 and \
       merged[['MHL NET(E)_prev', 'MHL GROSS(E)_prev']].sum().sum() == 0:
        return ""

    # --- Table formatting ---
    header_label = "Month" if group_col == "Month" else "Depot"
    lines = []
    header = f"{header_label:<<4} {'CY':>6} {'LY':>12} {'Var':>12}"
    subheader = f"{'':<4} {'| Net , Grs |':>10} {' Net , Grs |':>10} {'Net , Grs':>10}"
    underline = "-" * len(header)
    lines.extend([header, subheader, underline])

    # --- Row-wise EPK calculations ---
    for _, row in merged.iterrows():
        kms_cur = row['KMS_cur']
        kms_prev = row['KMS_prev']
        net_epk_cur   = row['MHL NET(E)_cur'] / kms_cur if kms_cur else 0
        gross_epk_cur = row['MHL GROSS(E)_cur'] / kms_cur if kms_cur else 0
        net_epk_prev  = row['MHL NET(E)_prev'] / kms_prev if kms_prev else 0
        gross_epk_prev= row['MHL GROSS(E)_prev'] / kms_prev if kms_prev else 0
        var_net       = net_epk_cur - net_epk_prev
        var_gross     = gross_epk_cur - gross_epk_prev

        lines.append(
            f"{str(row[group_col]):<4}"
            f" |{net_epk_cur:.2f},{gross_epk_cur:.2f}|"
            f" {net_epk_prev:.2f},{gross_epk_prev:.2f}|"
            f" {var_net:+.2f},{var_gross:+.2f}"
        )

    # --- Cumulative (CUM) row ---
    if show_cumulative:
        tot_kms_cur  = merged['KMS_cur'].sum()
        tot_kms_prev = merged['KMS_prev'].sum()
        tot_net_epk_cur   = merged['MHL NET(E)_cur'].sum() / tot_kms_cur if tot_kms_cur else 0
        tot_gross_epk_cur = merged['MHL GROSS(E)_cur'].sum() / tot_kms_cur if tot_kms_cur else 0
        tot_net_epk_prev  = merged['MHL NET(E)_prev'].sum() / tot_kms_prev if tot_kms_prev else 0
        tot_gross_epk_prev= merged['MHL GROSS(E)_prev'].sum() / tot_kms_prev if tot_kms_prev else 0
        tot_var_net       = tot_net_epk_cur - tot_net_epk_prev
        tot_var_gross     = tot_gross_epk_cur - tot_gross_epk_prev

        lines.append(underline)
        lines.append(
            f"{'CUM':<4}"
        f" |{tot_net_epk_cur:.2f},{tot_gross_epk_cur:.2f}|"
        f" {tot_net_epk_prev:.2f},{tot_gross_epk_prev:.2f}|"
        f" {tot_var_net:+.2f},{tot_var_gross:+.2f}"
        )
        lines.append(underline)

    return "\n".join(lines)


def monthwise_fp_mhl_pass_table(df_cur, df_prev, month_order=None, show_cumulative=True, group_col='Month'):
    """Monthwise or Depotwise MHL & FP PASS comparison table."""

    if month_order is None:
        month_order = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]

    # --- Aggregate current and previous data ---
    cur = df_cur.groupby(group_col).agg({
        'MHL PASS': 'sum',
        'FP PASS': 'sum'
    }).reset_index()

    prev = df_prev.groupby(group_col).agg({
        'MHL PASS': 'sum',
        'FP PASS': 'sum'
    }).reset_index()

    merged = pd.merge(cur, prev, on=group_col, how='outer', suffixes=('_cur','_prev')).fillna(0)

    # --- Sorting logic ---
    if group_col == 'Month':
        merged[group_col] = pd.Categorical(merged[group_col], categories=month_order, ordered=True)
        merged = merged.sort_values(group_col)
    else:
        merged = merged.sort_values(group_col)

    # --- Skip empty output if no data ---
    if merged[['MHL PASS_cur','FP PASS_cur']].sum().sum() == 0 and \
       merged[['MHL PASS_prev','FP PASS_prev']].sum().sum() == 0:
        return ""

    # --- Formatting setup ---
    header_label = "Month" if group_col == "Month" else "Depot"
    lines = []
    header = f"{header_label:<4} {'CY':>6} {'LY':>12} {'Var':>12}"
    subheader = f"{'':<4} {'| MHL , FP  |':>10} {' MHL , FP  |':>10} {'MHL , FP ':>10}"
   
    underline = "-" * len(header)
    lines.extend([header, subheader, underline])

    # --- Row-wise calculations ---
    for _, row in merged.iterrows():
        MHL_PASS_cur   = row['MHL PASS_cur'] / 100000
        FP_PASS_cur    = row['FP PASS_cur'] / 100000
        MHL_PASS_prev  = row['MHL PASS_prev'] / 100000
        FP_PASS_prev   = row['FP PASS_prev'] / 100000
        var_MHL_PASS   = MHL_PASS_cur - MHL_PASS_prev
        var_FP_PASS    = FP_PASS_cur - FP_PASS_prev

        lines.append(
            f"{str(row[group_col]):<4}"
            f" |{MHL_PASS_cur:6.2f},{FP_PASS_cur:6.2f}|"
            f" {MHL_PASS_prev:5.2f},{FP_PASS_prev:5.2f}|"
            f" {var_MHL_PASS:+5.2f},{var_FP_PASS:+5.2f}"
        )

    # --- Cumulative (CUM) row ---
    if show_cumulative:
        tot_MHL_PASS_cur   = merged['MHL PASS_cur'].sum() / 100000
        tot_FP_PASS_cur    = merged['FP PASS_cur'].sum() / 100000
        tot_MHL_PASS_prev  = merged['MHL PASS_prev'].sum() / 100000
        tot_FP_PASS_prev   = merged['FP PASS_prev'].sum() / 100000
        tot_var_MHL_PASS   = tot_MHL_PASS_cur - tot_MHL_PASS_prev
        tot_var_FP_PASS    = tot_FP_PASS_cur - tot_FP_PASS_prev

        lines.append(underline)
        lines.append(
            f"{'CUM':<4}"
            f" |{tot_MHL_PASS_cur:6.2f},{tot_FP_PASS_cur:6.2f}|"
            f" {tot_MHL_PASS_prev:5.2f},{tot_FP_PASS_prev:5.2f}|"
            f" {tot_var_MHL_PASS:+5.2f},{tot_var_FP_PASS:+5.2f}"


        )
        lines.append(underline)

    return "\n".join(lines)


def monthwise_net_gross_fp_epk_table(df_cur, df_prev, month_order=None, show_cumulative=True, group_col='Month'):
    """Monthwise or Depotwise Fare Paid NET & GROSS EPK comparison table."""

    if month_order is None:
        month_order = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]

    # --- Aggregate current and previous data ---
    cur = df_cur.groupby(group_col).agg({
        'FP NET(E)': 'sum',
        'FP GROSS(E)': 'sum',
        'KMS': 'sum'
    }).reset_index()

    prev = df_prev.groupby(group_col).agg({
        'FP NET(E)': 'sum',
        'FP GROSS(E)': 'sum',
        'KMS': 'sum'
    }).reset_index()

    merged = pd.merge(cur, prev, on=group_col, how='outer', suffixes=('_cur','_prev')).fillna(0)

    # --- Sorting logic ---
    if group_col == 'Month':
        merged[group_col] = pd.Categorical(merged[group_col], categories=month_order, ordered=True)
        merged = merged.sort_values(group_col)
    else:
        merged = merged.sort_values(group_col)

    # --- Skip empty tables ---
    if merged[['FP NET(E)_cur','FP GROSS(E)_cur']].sum().sum() == 0 and \
       merged[['FP NET(E)_prev','FP GROSS(E)_prev']].sum().sum() == 0:
        return ""

    # --- Formatting setup ---
    header_label = "Month" if group_col == "Month" else "Depot"
    lines = []
    header = f"{header_label:<4} {'CY':>6} {'LY':>12} {'Var':>12}"
    subheader = f"{'':<4} {'| Net , Grs |':>10} {' Net , Grs |':>10} {'Net , Grs':>10}"
    underline = "-" * len(header)
    lines.extend([header, subheader, underline])

    # --- Row-wise calculations ---
    for _, row in merged.iterrows():
        kms_cur = row['KMS_cur']
        kms_prev = row['KMS_prev']
        net_epk_cur   = row['FP NET(E)_cur'] / kms_cur if kms_cur else 0
        gross_epk_cur = row['FP GROSS(E)_cur'] / kms_cur if kms_cur else 0
        net_epk_prev  = row['FP NET(E)_prev'] / kms_prev if kms_prev else 0
        gross_epk_prev= row['FP GROSS(E)_prev'] / kms_prev if kms_prev else 0
        var_net   = net_epk_cur - net_epk_prev
        var_gross = gross_epk_cur - gross_epk_prev

        lines.append(
            f"{str(row[group_col]):<4}"
            f" |{net_epk_cur:.2f},{gross_epk_cur:.2f}|"
            f" {net_epk_prev:.2f},{gross_epk_prev:.2f}|"
            f" {var_net:+.2f},{var_gross:+.2f}"
        )

    # --- Cumulative (CUM) row ---
    if show_cumulative:
        tot_kms_cur  = merged['KMS_cur'].sum()
        tot_kms_prev = merged['KMS_prev'].sum()
        tot_net_epk_cur   = merged['FP NET(E)_cur'].sum() / tot_kms_cur if tot_kms_cur else 0
        tot_gross_epk_cur = merged['FP GROSS(E)_cur'].sum() / tot_kms_cur if tot_kms_cur else 0
        tot_net_epk_prev  = merged['FP NET(E)_prev'].sum() / tot_kms_prev if tot_kms_prev else 0
        tot_gross_epk_prev= merged['FP GROSS(E)_prev'].sum() / tot_kms_prev if tot_kms_prev else 0
        tot_var_net   = tot_net_epk_cur - tot_net_epk_prev
        tot_var_gross = tot_gross_epk_cur - tot_gross_epk_prev

        lines.append(underline)
        lines.append(
            f"{'CUM':<4}"
            f" |{tot_net_epk_cur:.2f},{tot_gross_epk_cur:.2f}|"
            f" {tot_net_epk_prev:.2f},{tot_gross_epk_prev:.2f}|"
            f" {tot_var_net:+.2f},{tot_var_gross:+.2f}"
        )
        lines.append(underline)

    return "\n".join(lines)

# Example: adjust monthwise_pass_table to skip empty:
def monthwise_pass_table(df_cur, df_prev, column='TOT PASS', show_cumulative=True, group_col='Month'):
    """
    Builds a formatted Month-wise or Depot-wise table for passenger counts.
    - If group_col='Month' → same as existing month-wise table
    - If group_col='DEPOT' → depot-wise summary (same formatting, includes CUM row)
    """

    cur = df_cur.groupby(group_col).agg({column: 'sum'}).reset_index()
    prev = df_prev.groupby(group_col).agg({column: 'sum'}).reset_index()

    merged = pd.merge(cur, prev, on=group_col, how='outer', suffixes=('_cur', '_prev')).fillna(0)

    # Month sorting logic (only if Month-based)
    if group_col == 'Month':
        merged[group_col] = pd.Categorical(merged[group_col], categories=FY_MONTH_ORDER, ordered=True)
        merged = merged.sort_values(group_col)
    else:
        merged = merged.sort_values(group_col)

    # Skip if no data at all
    if merged[f'{column}_cur'].sum() == 0 and merged[f'{column}_prev'].sum() == 0:
        return ""

    # Build formatted lines
    lines = []
    header_label = "Month" if group_col == "Month" else "Depot"
    header = f"{header_label:<4} {'CY':>9} {'LY':>12} {'Var':>12}"
    underline = "-" * len(header)
    lines.extend([header, underline])

    cum_cy = cum_ly = 0
    for _, r in merged.iterrows():
        cy = r[f'{column}_cur'] / 100000
        ly = r[f'{column}_prev'] / 100000
        var = cy - ly
        cum_cy += cy
        cum_ly += ly
        lines.append(f"{str(r[group_col]):<4} {cy:>12,.2f} {ly:>12,.2f} {var:>+12,.2f}")

    # Add cumulative row
    if show_cumulative:
        lines.append(underline)
        cum_var = cum_cy - cum_ly
        lines.append(f"CUM   {cum_cy:>12,.2f} {cum_ly:>12,.2f} {cum_var:>+12,.2f}")
        lines.append(underline)

    return "\n".join(lines)
# (Do same pattern for your other monthwise_* functions – return "" if sums zero)

# ------------------- Bot Handlers -------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scripts = ["SCH VS FLEET","ROUTE SCH","ACT vs ACT", "TGT vs ACT", "Route/Product"]
    reply_markup = build_keyboard_matrix(scripts, n_cols=3, prefix="SCRIPT|")
    await update.message.reply_text("📄 Select a report:", reply_markup=reply_markup)

async def script_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, script_name = query.data.split("|")
    context.user_data["report_type"] = script_name

    if "ACT vs ACT" in script_name:
        context.user_data["df_selected"] = df_act
        await show_depot_month_menu(update, context)
    elif "TGT vs ACT" in script_name:
        context.user_data["df_selected"] = df_tgt
        await show_depot_month_menu(update, context)
    
    elif "Route/Product" in script_name:
         
            await start_script3(update, context)
    elif "SCH VS FLEET" in script_name:
        context.user_data["df_selected"] = df_sch
        await show_depot_month_menu(update, context)
    elif "ROUTE SCH" in script_name:
         await route_sch(update, context)

# ------------------- Scripts 1 & 2 flow -------------------
async def show_depot_month_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Select Depot", callback_data="SELECT_DEPOT")],
        [InlineKeyboardButton("📅 Select Month", callback_data="SELECT_MONTH")]
    ])
    if update.callback_query:
        await update.callback_query.edit_message_text("Choose a filter:", reply_markup=reply_markup)
    else:
        await update.message.reply_text("Choose a filter:", reply_markup=reply_markup)

async def select_depot_or_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    df_selected = context.user_data["df_selected"]

    if query.data == "SELECT_DEPOT":
        depots = sorted(df_selected["DEPOT"].dropna().unique())
        reply_markup = build_keyboard_matrix(depots, n_cols=3, prefix="DEPOT|")
        await query.edit_message_text("📍 Select a Depot:", reply_markup=reply_markup)
    elif "MONTH" in query.data:
        depots = sorted(df_selected["DEPOT"].unique())
    
        # --- Month filter fix (Apr–Mar order) ---
        FY_MONTH_ORDER = ["APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC", "JAN", "FEB", "MAR"]
        available_months = df_selected["MONTH"].dropna().unique().tolist()
        months = [m for m in FY_MONTH_ORDER if m in available_months]

        reply_markup = build_keyboard_matrix(months, n_cols=4, prefix="MONTH|")
        await query.edit_message_text("📆 Select Month (Apr–Mar Order):", reply_markup=reply_markup)


async def depot_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, depot = query.data.split("|")
    df_selected = context.user_data["df_selected"]
    report_type = context.user_data["report_type"]
    df_filtered = df_selected[df_selected["DEPOT"]==depot]

    if "ACT vs ACT" in report_type:
        text = format_act_vs_act(df_filtered, depot=depot, all_months=True)
    elif "TGT vs ACT" in report_type:
        text = format_tgt_vs_act(df_filtered, depot=depot, all_months=True)
    else:
        text = format_sch_vs_fleet(df_filtered, depot=depot, all_months=True)
    await query.edit_message_text(text, parse_mode="Markdown")

async def month_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, month = query.data.split("|")
    df_selected = context.user_data["df_selected"]
    report_type = context.user_data["report_type"]
    df_filtered = df_selected[df_selected["MONTH"]==month]

    if "ACT vs ACT" in report_type:
        text = format_act_vs_act(df_filtered, month=month)
    elif "TGT vs ACT" in report_type:
        text = format_tgt_vs_act(df_filtered, month=month)
    else:
        text = format_sch_vs_fleet(df_filtered, month=month)
    await query.edit_message_text(text, parse_mode="Markdown")


async def start_script3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["filters"] = {}
    context.user_data["filter_index"] = 0
    await ask_next_filter(update, context)
async def ask_next_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    FILTER_ORDER = ["FY", "NMHL/MHL", "Month", "DEPOT",  "Product", "ROUTE","SER"]
    idx = context.user_data["filter_index"]
    if idx >= len(FILTER_ORDER):
        await send_result_script3(update, context)
        return
    col = FILTER_ORDER[idx]
    df = df_cache.copy()
    for fcol, fval in context.user_data["filters"].items():
        if fval != "All":
            df = df[df[fcol].astype(str) == fval]

    # Automatically select latest FY if not chosen
    if col == "FY" and "FY" not in context.user_data["filters"]:
        latest_fy = df["FY"].max()
        context.user_data["filters"]["FY"] = latest_fy
        context.user_data["filter_index"] += 1
        await ask_next_filter(update, context)
        return
    # Get unique options
    if col == "Month":
        opts = [m for m in FY_MONTH_ORDER if m in df[col].unique()]
    else:
        opts = sorted(df[col].dropna().astype(str).unique().tolist())
    opts.insert(0, "All")
    if len(opts) > 25:
        context.user_data["awaiting_text_for"] = col
        await update.effective_chat.send_message(
            f"There are many {col} values. Please type the {col} you want (or 'All')."
        )
    else:
        keyboard = []
        row = []
        for i, opt in enumerate(opts, 1):
            row.append(InlineKeyboardButton(opt, callback_data=f"{col}:{opt}"))
            if i % 3 == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        text = f"Select {col}:"
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=text, reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                text=text, reply_markup=InlineKeyboardMarkup(keyboard))

# ----- Handle typed text option -----
def build_telegram_message(*tables, code_block=True):
    non_empty = [t.strip() for t in tables if t and t.strip()]
    if not non_empty:
        return ""
    msg = "\n\n".join(non_empty)
    return f"```\n{msg}\n```" if code_block else msg
async def handle_text_script3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "awaiting_text_for" not in context.user_data:
        return
    col = context.user_data.pop("awaiting_text_for")
    val = update.message.text.strip()
    context.user_data["filters"][col] = val
    context.user_data["filter_index"] += 1
    await ask_next_filter(update, context)

async def button_script3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    col, val = query.data.split(":", 1)
    context.user_data["filters"][col] = val
    context.user_data["filter_index"] += 1
    await ask_next_filter(update, context)


# ----- Handle button option -----
async def send_result_script3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filters = context.user_data["filters"]
    df = df_cache.copy()

    # === Apply filters ===
    for col, val in filters.items():
        if col != "Month" and val != "All":
            df = df[df[col].astype(str) == val]

    # === Build previous FY df ===
    prev_filters = filters.copy()
    if "FY" in prev_filters:
        try:
            start, end = prev_filters["FY"].split("-")
            prev_filters["FY"] = f"{int(start)-1}-{int(end)-1}"
        except:
            pass

    df_prev = df_cache.copy()
    for col, val in prev_filters.items():
        if col != "Month" and val != "All":
            df_prev = df_prev[df_prev[col].astype(str) == val]

    # === Month filter ===
    if filters.get("Month") == "All":
        curr_fy = filters.get("FY")
        latest_month_num = df_cache[df_cache["FY"] == curr_fy]["DATE"].max().month
        df = df[(df["FY"] == curr_fy) & (df["DATE"].dt.month.between(4, latest_month_num))]
        df_prev = df_prev[(df_prev["FY"] == prev_filters["FY"]) &
                          (df_prev["DATE"].dt.month.between(4, latest_month_num))]
    else:
        df = df[df["Month"].astype(str) == filters["Month"]]
        df_prev = df_prev[df_prev["Month"].astype(str) == filters["Month"]]

    # === No data check ===
    if df.empty:
        await update.effective_chat.send_message("No data matches your selection.")
        return

    # === Dynamic grouping ===
    group_by_col = "Month" if filters.get("Month") == "All" else "DEPOT"
    show_cumulative = True  # Keep CUM row for both cases

    # === Monthwise/Depotwise tables ===
    net_gross_epk_table = monthwise_net_gross_epk_table(df, df_prev, show_cumulative=show_cumulative, group_col=group_by_col)
    fp_epk_table        = monthwise_net_gross_fp_epk_table(df, df_prev, show_cumulative=show_cumulative, group_col=group_by_col)
    mhl_epk_table       = monthwise_net_gross_mhl_epk_table(df, df_prev, show_cumulative=show_cumulative, group_col=group_by_col)
    fp_mhl_pass_table   = monthwise_fp_mhl_pass_table(df, df_prev, show_cumulative=show_cumulative, group_col=group_by_col)
    kms_pass_table = monthwise_kms_pass_table(df, df_prev, show_cumulative=show_cumulative, group_col=group_by_col)

    tot_km_table    = monthwise_pass_table(df, df_prev, "KMS", show_cumulative, group_col=group_by_col)
    tot_NETE_table  = monthwise_pass_table(df, df_prev, "NET (E)", show_cumulative, group_col=group_by_col)
    tot_grs_table   = monthwise_pass_table(df, df_prev, "GROSS (E)", show_cumulative, group_col=group_by_col)
    tot_pass_table  = monthwise_pass_table(df, df_prev, "TOT PASS", show_cumulative, group_col=group_by_col)
    mhl_pass_table  = monthwise_pass_table(df, df_prev, "MHL PASS", show_cumulative, group_col=group_by_col)
    fp_pass_table   = monthwise_pass_table(df, df_prev, "FP PASS", show_cumulative, group_col=group_by_col)
    mhl_grs_table   = monthwise_pass_table(df, df_prev, "MHL GROSS(E)", show_cumulative, group_col=group_by_col)
    mhl_net_table   = monthwise_pass_table(df, df_prev, "MHL NET(E)", show_cumulative, group_col=group_by_col)
    fp_grs_table    = monthwise_pass_table(df, df_prev, "FP GROSS(E)", show_cumulative, group_col=group_by_col)
    fp_net_table    = monthwise_pass_table(df, df_prev, "FP NET(E)", show_cumulative, group_col=group_by_col)

    # === NMHL/MHL filter logic ===
    nmhl_filter = filters.get("NMHL/MHL", "All").upper()
    include_mhl = not (nmhl_filter == "NMHL")

    if not include_mhl:
        mhl_epk_table = ""
        fp_epk_table = ""
        fp_net_table = ""
        fp_grs_table = ""
        mhl_pass_table = ""
        mhl_grs_table = ""
        mhl_net_table = ""
        fp_mhl_pass_table = ""


    # === Heading ===
    heading_parts = [
        f"FY: {filters.get('FY','All')}",
        f"Month: {filters.get('Month','All')}",
        f"DEPOT: {filters.get('DEPOT','All')}",
        f"SER: {filters.get('SER','All')}",
        f"Product: {filters.get('Product','All')}",
        f"ROUTE: {filters.get('ROUTE','All')}"
    ]
    heading_text = ", ".join(heading_parts)
    table_heading = f"\n📋 *Selection:* {heading_text}\n"
    # === Message 1 ===
    msg1 = build_telegram_message(
        f"{table_heading}",

        f"📊 Tot KMs & Psgnrs. (in Lks):\n{kms_pass_table}",
        f"📊 Tot Net Earnings (in Lks):\n{tot_NETE_table}",
        f"📊 Tot Gross Earnings (in Lks):\n{tot_grs_table}",
        f"📊 FP Net Earnings (in Lks):\n{fp_net_table}" if include_mhl else "",
        f"📊 FP Gross Earnings (in Lks):\n{fp_grs_table}" if include_mhl else "",
        f"📊 MHL Net Earnings (in Lks):\n{mhl_net_table}" if include_mhl else "",
        f"📊 MHL Gross Earnings (in Lks):\n{mhl_grs_table}" if include_mhl else ""
    )

    # === Message 2 ===
    msg2 = build_telegram_message(
        f"{table_heading}",
        f"📊 Tot EPK Net,Gross (in Rs.):\n{net_gross_epk_table}",
        f"📊 Fare Paid EPK Net,Gross (in Rs.):\n{fp_epk_table}" if include_mhl else "",
        f"📊 MHL EPK Net,Gross (in Rs.):\n{mhl_epk_table}" if include_mhl else "",
        f"📊 FP & MHL Passengers (in Lks):\n{fp_mhl_pass_table}" if include_mhl else ""
    )

    # === Send messages ===
    if msg1:
        await update.effective_chat.send_message(msg1, parse_mode="Markdown")
    if msg2:
        await update.effective_chat.send_message(msg2, parse_mode="Markdown")


def load_data():
    df = pd.read_excel(EXCEL_ACT, sheet_name="rts")
    expected_cols = ["Month", "DEPOT", "NO SCH", "NO SER", "SCH KMS", "Product", "ROUTE", "NMHL/MHL"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in Excel: {missing}")

    # Convert Month column to abbreviation
    if pd.api.types.is_numeric_dtype(df["Month"]):
        df["Month"] = df["Month"].apply(lambda x: pd.Timestamp(2025, int(x), 1).strftime('%b'))
    elif pd.api.types.is_datetime64_any_dtype(df["Month"]):
        df["Month"] = df["Month"].dt.strftime('%b')
    else:
        df["Month"] = df["Month"].astype(str).str[:3]

    return df

# ---------------- TABLE FORMATTING WITH BORDERS ----------------

COLUMN_NAMES = ["Depot", "Prod.", "Route", "Hld","Sch", "Ser", "Sh.kms"]
ORIGINAL_COLUMNS = ["DEPOT", "Product", "ROUTE", "Fleet", "NO SCH", "NO SER", "SCH KMS"]

def calculate_col_widths(df: pd.DataFrame, columns: list) -> dict:
    widths = {}
    for orig, new in zip(ORIGINAL_COLUMNS, columns):
        max_len = max(df[orig].astype(str).apply(len).max(), len(new))
        widths[new] = max_len
    return widths

def create_border_line(col_widths, left='┌', middle='┬', right='┐', line_char='─'):
    return left + middle.join([line_char * (w + 2) for w in col_widths.values()]) + right

def create_separator_line(col_widths, left='├', middle='┼', right='┤', line_char='─'):
    return left + middle.join([line_char * (w + 2) for w in col_widths.values()]) + right

def create_bottom_line(col_widths, left='└', middle='┴', right='┘', line_char='─'):
    return left + middle.join([line_char * (w + 2) for w in col_widths.values()]) + right

def add_table_heading(context, filter_type: str, selection: str) -> str:
    month = context.user_data.get("month", "")
    first_filter = context.user_data.get("first_filter", "")
    heading = f"📊Report: Month={month}, Filter={first_filter}, {filter_type}={selection}\n"
    heading += "=" * 40 + "\n"
    return heading

def df_to_bordered_text(df: pd.DataFrame, context=None, filter_type="", selection="") -> str:
    """
    Generates a neatly aligned text table:
    Dept | Prod | Route | Hld | Sch | Ser | Sh.kms | AVU
    """

    # --- Heading ---
    text = ""
    if context:
        text += add_table_heading(context, filter_type, selection)

    # --- Clean data ---
    df = df.fillna(0).copy()

    # --- Compute AVU ---
    df["AVU"] = df.apply(
        lambda r: (r["SCH KMS"] / r["Fleet"]) if float(r["Fleet"]) != 0 else 0,
        axis=1
    )

    # --- Columns ---
    columns = ["Dept", "Prod", "Route", "Hld", "Sch", "Ser", "Sh.kms", "AVU"]
    orig_cols = ["DEPOT", "Product", "ROUTE", "Fleet", "NO SCH", "NO SER", "SCH KMS"]

    # --- Column widths ---
    col_widths = {}
    for orig, new in zip(orig_cols + ["AVU"], columns):
        if new == "AVU":
            max_len = max(len("AVU"), df["AVU"].apply(lambda x: len(f"{x:.0f}")).max())
        else:
            max_len = max(len(new), df[orig].astype(str).apply(len).max())
        col_widths[new] = max_len

    # --- Reduce width padding for numeric columns ---
    # Make numeric columns (4–7) slightly tighter
    for key in ["Hld", "Sch", "Ser", "Sh.kms", "AVU"]:
        col_widths[key] = max(3, col_widths[key] - 1)

    widths_list = [col_widths[c] for c in columns]
    total_width = sum(widths_list) + (len(columns) - 1)  # spaces between cols

    # --- Formatting helpers ---
    def format_row(values):
        left_cols = [0, 1, 2]  # left aligned columns
        # Reduce extra spaces after numeric columns
        parts = []
        for i, v in enumerate(values):
            if i in left_cols:
                parts.append(f"{str(v):<{widths_list[i]}}")
            else:
                parts.append(f"{v:>{widths_list[i]}}")
        return " ".join(parts)

    # --- Header ---
    header = format_row(columns)
    underline = "=" * total_width
    text += underline + "\n" + header + "\n" + underline + "\n"

    # --- Group by Product ---
    grouped = df.groupby("Product")

    for product, group_df in grouped:
        for _, row in group_df.iterrows():
            values = [
                row["DEPOT"],
                row["Product"],
                row["ROUTE"],
                int(row["Fleet"]),
                int(row["NO SCH"]),
                int(row["NO SER"]),
                int(row["SCH KMS"]),
                int(row["AVU"]),
            ]
            text += format_row(values) + "\n"

        # --- Subtotal ---
        subtotal = group_df[["Fleet", "NO SCH", "NO SER", "SCH KMS"]].astype(float).sum()
        avu_total = subtotal["SCH KMS"] / subtotal["Fleet"] if subtotal["Fleet"] else 0

        subtotal_label = f"{product} TOTAL"
        left_width = sum(widths_list[:3]) + 2
        subtotal_vals = [
            int(subtotal["Fleet"]),
            int(subtotal["NO SCH"]),
            int(subtotal["NO SER"]),
            int(subtotal["SCH KMS"]),
            int(avu_total),
        ]
        subtotal_text = " ".join(f"{v:>{w}}" for v, w in zip(subtotal_vals, widths_list[3:]))
        text += f"{subtotal_label:<{left_width}} {subtotal_text}\n"
        text += "-" * total_width + "\n"

    # --- Grand total ---
    grand = df[["Fleet", "NO SCH", "NO SER", "SCH KMS"]].astype(float).sum()
    grand_avu = grand["SCH KMS"] / grand["Fleet"] if grand["Fleet"] else 0
    grand_label = "Grand Total"
    left_width = sum(widths_list[:3]) + 2
    grand_vals = [
        int(grand["Fleet"]),
        int(grand["NO SCH"]),
        int(grand["NO SER"]),
        int(grand["SCH KMS"]),
        int(grand_avu),
    ]
    grand_text = " ".join(f"{v:>{w}}" for v, w in zip(grand_vals, widths_list[3:]))
    text += f"{grand_label:<{left_width}} {grand_text}\n"
    text += "=" * total_width + "\n"

    return text

def split_text_markdown(text, max_chars=4000):
    lines = text.split("\n")
    chunks = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > max_chars:
            chunks.append(current)
            current = ""
        current += line + "\n"
    if current:
        chunks.append(current)
    if chunks:
        chunks[0] = "```\n" + chunks[0].lstrip("`\n")
        chunks[-1] = chunks[-1].rstrip("`\n") + "\n```"
    return chunks

# ---------------- TELEGRAM HANDLERS ----------------
async def route_sch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    df = load_data()
    months = df["Month"].dropna().unique().tolist()
    months_per_row = 4
    buttons = []
    for i in range(0, len(months), months_per_row):
        row = [
            InlineKeyboardButton(m, callback_data=f"month|{m}")
            for m in months[i:i + months_per_row]
        ]
        buttons.append(row)    
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("Select Month:", reply_markup=InlineKeyboardMarkup(buttons))

async def handle_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, month = query.data.split("|")
    context.user_data["month"] = month

    buttons = [
        [InlineKeyboardButton("MHL", callback_data="first_filter|MHL")],
        [InlineKeyboardButton("NMHL", callback_data="first_filter|NMHL")],
        [InlineKeyboardButton("All", callback_data="first_filter|All")]
    ]
    await query.edit_message_text(
        text=f"Month selected: *{month}*\nChoose NMHL/MHL filter:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_first_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, first_filter = query.data.split("|")
    context.user_data["first_filter"] = first_filter

    df = load_data()
    month = context.user_data.get("month")

    if first_filter in ["MHL", "NMHL"]:
        df = df[df["NMHL/MHL"] == first_filter]

    context.user_data["filtered_df"] = df[df["Month"] == month].reset_index(drop=True)

    buttons = [
        [InlineKeyboardButton("Depot", callback_data="filter|DEPOT")],
        [InlineKeyboardButton("Route", callback_data="filter|ROUTE")],
        [InlineKeyboardButton("Product", callback_data="filter|Product")]
    ]
    await query.edit_message_text(
        text=f"Month: *{month}*, Filter: *{first_filter}*\nChoose filter type:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_filter_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, filter_type = query.data.split("|")
    context.user_data["filter_type"] = filter_type

    df = context.user_data.get("filtered_df")
    if df is None or df.empty:
        await query.edit_message_text(text="No data available for selected Month/NMHL/MHL filter.")
        return

    options = df[filter_type].dropna().unique().tolist()

    # ---- For ROUTE filter: sort alphabetically and show as matrix ----
    if filter_type == "ROUTE":
        options = sorted(options)

        # Define how many routes per row (adjust if needed)
        routes_per_row = 4

        # Create buttons in matrix form
        buttons = []
        for i in range(0, len(options), routes_per_row):
            row = [
                InlineKeyboardButton(opt, callback_data=f"select|{opt}")
                for opt in options[i:i + routes_per_row]
            ]
            buttons.append(row)

        await query.edit_message_text(
            text=f"Select {filter_type} (sorted A–Z):",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    # ---- For other filters: default single-column layout ----
    options = sorted(options)
    routes_per_row = 4

    buttons = []
    for i in range(0, len(options), routes_per_row):
            row = [
                InlineKeyboardButton(opt, callback_data=f"select|{opt}")
                for opt in options[i:i + routes_per_row]
            ]
            buttons.append(row)
    await query.edit_message_text(
        text=f"Select {filter_type}:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, selection = query.data.split("|")
    context.user_data["selection"] = selection

    df = context.user_data.get("filtered_df")
    filter_type = context.user_data.get("filter_type")

    filtered_df = df[df[filter_type] == selection]
    if filtered_df.empty:
        await query.edit_message_text("No data available for this selection.")
        return

    table_text = df_to_bordered_text(filtered_df, context=context, filter_type=filter_type, selection=selection)

    chunks = split_text_markdown(table_text)
    for i, chunk in enumerate(chunks):
        if i == 0:
            await query.edit_message_text(chunk, parse_mode="Markdown")
        else:
            await query.message.reply_text(chunk, parse_mode="Markdown")
# ------------------- Main -------------------
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Start
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(script_selected, pattern=r"^SCRIPT\|"))

    # Scripts 1 & 2 handlers
    app.add_handler(CallbackQueryHandler(select_depot_or_month, pattern=r"^SELECT_"))
    app.add_handler(CallbackQueryHandler(depot_selected, pattern=r"^DEPOT\|"))
    app.add_handler(CallbackQueryHandler(month_selected, pattern=r"^MONTH\|"))

    #Script 3 handlers
    app.add_handler(CallbackQueryHandler(button_script3, pattern=r"^[^|:]+:.+"))#
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_script3))#
    

    app.add_handler(CallbackQueryHandler(handle_month, pattern=r"^month\|"))
    app.add_handler(CallbackQueryHandler(handle_first_filter, pattern=r"^first_filter\|"))
    app.add_handler(CallbackQueryHandler(handle_filter_type, pattern=r"^filter\|"))
    app.add_handler(CallbackQueryHandler(handle_selection, pattern=r"^select\|"))

    app.run_polling()

if __name__ == "__main__":
    main()
