# Simulate Hidden Causes
# Measure FP ability to test versus greedy algorithms

import json
import tempfile
import os
import subprocess
import pandas
import numpy
import timeout
import time
import gc
import traceback
import sys
import random
import string
from functools import reduce

# Try to import yaml, provide helpful error if missing
try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. Run: pip install pyyaml")
    sys.exit(1)

########
### Constants

OPEN_VAL = "(Open)"
DESC_DELIM = ":: "
START_DATE = "2019-01-01"
END_DATE = "2020-01-01"
NEXT_DATE = "1/1/2020"

########
### Config Loading

def load_config(config_path: str = None) -> dict:
    """Load YAML config file or return defaults."""
    if config_path is None:
        # Look for config in same directory as script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "sim_config.yaml")

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            print(f"Loaded config from: {config_path}")
            return config
    else:
        print(f"Config not found at {config_path}, using defaults")
        return get_default_config()

def get_default_config() -> dict:
    """Return default configuration (backward compatible with original)."""
    return {
        "fpConsolePath": r"G:\My Drive\Scuba\Publish Location\XPConsoleProfiles\win-x64\FactorPrismConsoleXP.exe",
        "testConfigs": [
            {"numHierarchies": 2, "levels": 2, "modes": 2},
            {"numHierarchies": 2, "levels": 2, "modes": 3},
        ],
        "solverMethods": [
            "GreedyTopDown", "GreedyBottomUp", "FPLP",
            "FPIteratedRegression", "FPIteratedRegression_Log", "FPOrthMP"
        ],
        "causesPerRun": [1, 3, 5],
        "numRunsPerSetting": 100,
        "timeoutSeconds": 10,
        "simulationParams": {
            "startMean": 10000,
            "startSD": 2000,
            "effectSD": 0.05,
            "effectTermSDPct": 0.1,
            "noiseSD": 0.001,
            "randomSeed": 2022,
        },
        "outputPath": r"C:\Users\david\OneDrive\Desktop\simOut.csv",
        "tempDirectory": r"Z:\TEMP",
    }

########
### Hierarchy Generation

def rand_string(num_chars: int = 5) -> str:
    """Generate random uppercase string for hierarchy values."""
    return ''.join(random.choices(string.ascii_uppercase, k=num_chars))

def generate_hierarchy_data(num_hierarchies: int, levels: int, modes: int, rng) -> dict:
    """
    Generate complete hierarchy structure for simulation.

    Args:
        num_hierarchies: Number of independent dimensions (e.g., 2 for Product + Geography)
        levels: Depth of each hierarchy (e.g., 2 for Category > SubCategory)
        modes: Branching factor / children per node
        rng: numpy random generator

    Returns:
        dict with:
            - columns: list of all column names (flattened)
            - hier_columns: list of column names per hierarchy
            - schema_df: DataFrame with all leaf-level combinations
            - levels_df: DataFrame with all factor combinations (including Open)
            - hier_labels: labels for FP config
            - hier_table: 5x5 table for FP config
    """
    # Generate column names for each hierarchy
    # E.g., 2 hierarchies with 2 levels each: [["H0_L0", "H0_L1"], ["H1_L0", "H1_L1"]]
    hier_columns = []
    for h in range(num_hierarchies):
        cols = [f"H{h}_L{l}" for l in range(levels)]
        hier_columns.append(cols)

    # Flatten for total columns list
    all_columns = [col for hier in hier_columns for col in hier]

    # Generate data for each hierarchy separately, then cross-product
    hier_data_rows = []
    hier_level_rows = []

    for h_idx, h_cols in enumerate(hier_columns):
        data_rows = []  # Leaf-level rows only
        level_rows = []  # All aggregation levels including Open

        # Recursive function to build hierarchy tree
        def build_tree(current_row: dict, level_idx: int):
            # Add current state to level rows (for factor combinations)
            level_rows.append(current_row.copy())

            if level_idx >= levels:
                # Leaf level - add to data rows
                data_rows.append(current_row.copy())
                return

            # Generate children at this level
            col_name = h_cols[level_idx]
            for _ in range(modes):
                child_row = current_row.copy()
                child_row[col_name] = rand_string(8)
                build_tree(child_row, level_idx + 1)

        # Start with all Open values
        start_row = {col: OPEN_VAL for col in h_cols}
        build_tree(start_row, 0)

        hier_data_rows.append(data_rows)
        hier_level_rows.append(level_rows)

    # Cross-product all hierarchies for leaf data
    def cross_product(list_of_row_lists):
        """Cross-product multiple lists of row dicts."""
        if len(list_of_row_lists) == 1:
            return list_of_row_lists[0]

        result = []
        first = list_of_row_lists[0]
        rest = cross_product(list_of_row_lists[1:])

        for row1 in first:
            for row2 in rest:
                result.append({**row1, **row2})

        return result

    schema_rows = cross_product(hier_data_rows)
    levels_rows = cross_product(hier_level_rows)

    # Create DataFrames
    schema_df = pandas.DataFrame(schema_rows, columns=all_columns)
    schema_df["Date"] = START_DATE
    schema_df["Units"] = 0.0

    levels_df = pandas.DataFrame(levels_rows, columns=all_columns)

    # Build hier_labels and hier_table for FP config
    hier_labels = [f"Hier_{h}" for h in range(num_hierarchies)]

    # FP expects 5x5 table - pad with empty strings
    hier_table = []
    for h_cols in hier_columns:
        row = h_cols + [""] * (5 - len(h_cols))
        hier_table.append(row[:5])
    # Pad to 5 rows
    while len(hier_table) < 5:
        hier_table.append(["", "", "", "", ""])

    return {
        "columns": all_columns,
        "hier_columns": hier_columns,
        "schema_df": schema_df,
        "levels_df": levels_df,
        "hier_labels": hier_labels[:5],  # Max 5 hierarchies
        "hier_table": hier_table[:5],
        "config_name": f"{num_hierarchies}h_{levels}L_{modes}m",
    }

########
### Simulation

def create_simulation_runner(config: dict):
    """Create a simulation runner with the given config."""

    sim_params = config.get("simulationParams", {})
    start_mean = sim_params.get("startMean", 10000)
    start_sd = sim_params.get("startSD", 2000)
    effect_sd = sim_params.get("effectSD", 0.05)
    effect_term_sd_pct = sim_params.get("effectTermSDPct", 0.1)
    noise_sd = sim_params.get("noiseSD", 0.001)

    fp_console_path = config["fpConsolePath"]
    solver_methods = config["solverMethods"]
    temp_loc = config.get("tempDirectory", r"Z:\TEMP")
    timeout_seconds = config.get("timeoutSeconds", 10)

    # Create RNG with seed
    seed = sim_params.get("randomSeed", 2022)
    rng = numpy.random.default_rng(seed)

    @timeout.timeout(timeout_seconds)
    def run_simulation(hier_data: dict, num_causes: int) -> dict:
        """Run simulation for a given hierarchy configuration."""

        columns = hier_data["columns"]
        schema_df = hier_data["schema_df"]
        levels_df = hier_data["levels_df"]
        hier_labels = hier_data["hier_labels"]
        hier_table = hier_data["hier_table"]

        # Build FP settings dict
        the_setting_dict = {
            "SelectedStartDateText": START_DATE,
            "SelectedEndDateText": END_DATE,
            "DateFieldName": "Date",
            "RollupSelection": "",
            "DataFieldName": "Units",
            "dataUnits": "Units",
            "CSVFilePath": "",
            "HierLabels": hier_labels,
            "FullHierTable": hier_table,
            "SolverMethodToUse": "",
            "CSVOutputType": "Legacy",
            "OutFilePath": ""
        }

        # Create temp files
        temp_in_csv = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv", dir=temp_loc)
        temp_out_csv = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv", dir=temp_loc)
        temp_json = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json", dir=temp_loc)

        temp_in_csv.close()
        temp_out_csv.close()
        temp_json.close()

        try:
            # Create starting data
            df = schema_df.copy()
            df["Units"] = rng.normal(start_mean, start_sd, size=len(df))

            # Create next period with noise
            df_next = df.copy()
            df_next["Date"] = NEXT_DATE
            df_next["Units"] = (df_next["Units"] * (1 + rng.normal(0, noise_sd, size=len(df_next)))).clip(lower=0)

            # Create effects, track actuals
            cause_indices = rng.choice(len(levels_df), size=num_causes)

            impacts_df = levels_df.copy()
            impacts_df["Impact"] = 0.0

            for ind in numpy.atleast_1d(cause_indices):
                cause_level = levels_df.loc[ind]
                cause_impact_pct = rng.normal(0, effect_sd)

                # Build match mask
                match_mask = pandas.Series(True, index=df_next.index)
                for col in columns:
                    val = cause_level[col]
                    if val != OPEN_VAL:
                        match_mask &= df_next[col] == val

                if not match_mask.any():
                    continue

                per_row_effect = rng.normal(
                    cause_impact_pct,
                    abs(cause_impact_pct) * effect_term_sd_pct,
                    size=match_mask.sum(),
                )

                current_units = df_next.loc[match_mask, "Units"]
                new_units = (current_units * (1 + per_row_effect)).clip(lower=0)
                df_next.loc[match_mask, "Units"] = new_units
                impacts_df.loc[ind, "Impact"] = (new_units - current_units).sum()

            # Combine periods
            df = pandas.concat([df, df_next], axis=0)
            df.to_csv(temp_in_csv.name, index=False)

            # Run each solver
            out_dict = {}
            for solver_method in solver_methods:
                the_setting_dict["CSVFilePath"] = temp_in_csv.name
                the_setting_dict["SolverMethodToUse"] = solver_method
                the_setting_dict["OutFilePath"] = temp_out_csv.name

                with open(temp_json.name, mode="w+", encoding="utf-8") as f:
                    json.dump(the_setting_dict, f)

                subprocess.run([fp_console_path, temp_json.name], check=True)
                gc.collect()

                out_dict[solver_method] = score_result(temp_out_csv.name, impacts_df, columns)

            return out_dict

        finally:
            for temp_path in (temp_in_csv.name, temp_out_csv.name, temp_json.name):
                try:
                    os.unlink(temp_path)
                except FileNotFoundError:
                    pass

    return run_simulation, rng

########
### Scoring

def score_result(result_csv_path: str, true_impact_df: pandas.DataFrame, columns: list) -> float:
    """Score solver output against known true impacts."""

    result_df = pandas.read_csv(result_csv_path)
    total_abs_impact = true_impact_df["Impact"].abs().sum()

    if total_abs_impact == 0:
        return 0.0

    def build_desc(row: pandas.Series) -> str:
        parts = [row[col] for col in columns if row[col] != OPEN_VAL]
        return DESC_DELIM.join(parts) if parts else "Overall"

    true_with_desc = true_impact_df.copy()
    true_with_desc["Description"] = true_impact_df.apply(build_desc, axis=1)

    net_by_desc = result_df.groupby("Description")["Net Impact"].sum()
    result_impacts = true_with_desc["Description"].map(net_by_desc).fillna(0)

    aligned = (
        (true_with_desc["Impact"] != 0)
        & (result_impacts != 0)
        & (numpy.sign(true_with_desc["Impact"]) == numpy.sign(result_impacts))
    )

    captured = numpy.where(
        aligned,
        numpy.minimum(true_with_desc["Impact"].abs(), result_impacts.abs()),
        0,
    ).sum()

    return captured / total_abs_impact

########
### Main Execution

def main():
    # Load config (from command line arg or default location)
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    config = load_config(config_path)

    # Extract settings
    test_configs = config["testConfigs"]
    causes_per_run = config["causesPerRun"]
    num_runs = config["numRunsPerSetting"]
    solver_methods = config["solverMethods"]
    output_path = config["outputPath"]

    # Create simulation runner
    run_simulation, rng = create_simulation_runner(config)

    # Pre-generate hierarchy data for each test config
    print("Generating hierarchy structures...")
    hier_data_list = []
    for tc in test_configs:
        hd = generate_hierarchy_data(
            tc["numHierarchies"],
            tc["levels"],
            tc["modes"],
            rng
        )
        hier_data_list.append(hd)
        leaves = len(hd["schema_df"])
        levels = len(hd["levels_df"])
        print(f"  {hd['config_name']}: {leaves} leaves, {levels} factor combinations")

    # Calculate total runs
    total_runs = num_runs * len(test_configs) * len(causes_per_run) * len(solver_methods)
    print(f"\nStarting {total_runs} total simulation runs...")

    # Run simulations
    run_index = 0
    results = []

    for run_num in range(num_runs):
        for hier_data in hier_data_list:
            for num_causes in causes_per_run:
                config_name = hier_data["config_name"]
                print(f"----- Run {run_index + 1} of {total_runs} ({config_name}, {num_causes} causes)")

                try:
                    sim_result = run_simulation(hier_data, num_causes)

                    for method in solver_methods:
                        results.append({
                            "config": config_name,
                            "numCauses": num_causes,
                            "method": method,
                            "accuracy": sim_result[method],
                        })
                        run_index += 1

                except Exception as e:
                    print(f"Error: {type(e).__name__}: {e}")
                    traceback.print_exc()
                    time.sleep(5)
                    continue

    # Save results
    out_df = pandas.DataFrame(results)
    out_df.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")

if __name__ == "__main__":
    main()
