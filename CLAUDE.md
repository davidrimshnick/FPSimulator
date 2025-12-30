# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FPSimulator is a Python benchmarking tool that generates synthetic test simulations to measure FactorPrism's ability to identify hidden causes versus greedy algorithms. It creates multi-dimensional hierarchical data, injects known impacts, and scores various solver algorithms on their accuracy.

**Related Repository:** `../FactorPrismGit` contains the FactorPrism C#/.NET codebase that this simulator benchmarks.

## What is FactorPrism?

FactorPrism is a factor decomposition toolkit that answers: "What caused this change?" When you observe a metric change over time (e.g., sales increased 20%), FactorPrism decomposes it into contributing factors across hierarchical segments.

**Example:** If total sales increased by $150,000:
- $90,000 from Product A growth in East Region
- $45,000 from Product B expansion in West Region
- $15,000 from market dynamics

The simulator tests how accurately different algorithms can recover these "hidden causes" when they're synthetically injected.

## Commands

**Run simulation:**
```bash
python imp_simulator.py                    # Uses default config (sim_config.yaml)
python imp_simulator.py /path/to/config    # Uses custom config file
```

**Install dependencies:**
```bash
pip install pyyaml pandas numpy
```

## Solver Algorithms

The simulator benchmarks these FactorPrism solvers (defined in `../FactorPrismGit/FactorPrismSharedCore/solvers/`):

| Solver | Description |
|--------|-------------|
| **GreedyTopDown** | Cascades factors from ancestors to leaves recursively. Fast baseline. |
| **GreedyBottomUp** | Trivial solution using leaf ratios, ancestor factors set to 1.0. |
| **FPLP** | Linear Programming using OR-Tools GLOP. Works in log space. |
| **FPMatchingPursuit** | Iterative weighted-median regression with F-test significance filtering. |
| **FPMatchingPursuit_Log** | Log-space variant of matching pursuit. |
| **FPOrthMP** | Orthogonal Matching Pursuit - recomputes full regression each iteration. |
| **FPOrthMP_Log** | Log-space variant of orthogonal matching pursuit. |

## Architecture

### Core Workflow

1. **Config Loading** - Reads `sim_config.yaml` for parameters and paths
2. **Hierarchy Generation** - Creates synthetic multi-level categorical hierarchies
3. **Simulation** - For each test configuration:
   - Generates baseline data with random normal distribution
   - Creates next period with noise
   - Injects hidden causes (ground truth impacts) at specific hierarchy segments
   - Runs each solver algorithm via subprocess to FactorPrismConsoleXP.exe
   - Scores output accuracy against known causes
4. **Results** - Outputs CSV with accuracy per solver/config combination

### Key Files

- `imp_simulator.py` - Main simulation engine containing all core logic
- `sim_config.yaml` - YAML configuration for paths, test parameters, and solver methods
- `timeout.py` - Threading-based timeout decorator for subprocess calls

### Data Flow

```
sim_config.yaml → Hierarchy Generation → Baseline + Impacts
                                              ↓
                                    For each solver method:
                                      └─ Write temp CSV/JSON config
                                      └─ Call FactorPrismConsoleXP.exe
                                      └─ Parse output CSV
                                      └─ Score vs ground truth
                                              ↓
                                    Results CSV (accuracy per method)
```

### FactorPrismConsoleXP.exe Interface

**Input:** JSON configuration file with:
- `CSVFilePath` - Path to input data CSV
- `OutFilePath` - Path for output results CSV
- `SolverMethodToUse` - Algorithm name (e.g., "FPMatchingPursuit")
- `HierLabels` - Hierarchy field names
- `FullHierTable` - Nested hierarchy structure
- Date range, data field names, thresholds

**Output:** CSV with columns for Description, Impact, Multipliers, Raw Data

### Key Constants

- `OPEN_VAL = "(Open)"` - Represents "all values" at a hierarchy level (matches FactorPrism convention)
- `DESC_DELIM = ":: "` - Separator for cause description strings
- Leaf count formula: `(modes^levels)^numHierarchies`

## Configuration (sim_config.yaml)

Key sections:
- `fpConsolePath` - Path to FactorPrism executable
- `testConfigs` - List of hierarchy configurations (numHierarchies, levels, modes)
- `solverMethods` - Algorithms to benchmark
- `causesPerRun` - Number of hidden causes to inject per simulation
- `numRunsPerSetting` - Iterations per configuration
- `timeoutSeconds` - Subprocess timeout
- `outputPath` - Results CSV location

## Scoring

The `score_result()` function compares solver output against known injected causes:
- Aligns results by description strings (hierarchy segment identifiers)
- Calculates accuracy as the fraction of actual impact correctly identified
- Returns 0-1 score where 1.0 = perfect recovery of hidden causes
