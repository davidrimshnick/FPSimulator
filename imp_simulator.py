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

########
### Paramaters

FPConsolePath = r"G:\My Drive\Scuba\Publish Location\XPConsoleProfiles\win-x64\FactorPrismConsoleXP.exe"

startMean = 10000
startSD = 2000
effectSD = .05 # effect size small so interaction terms don't matter; centered at 0 and percent of original value as SD
effectTermSDPct = .1 # how big the effect differs in subcategories as percent of effect
noiseSD = .001

causesPerRun = [1, 3, 5]
numRunsPerSetting = 100
solverMethods = ["GreedyTopDown", "GreedyBottomUp", "FPLP", "FPIteratedRegression", "FPIteratedRegression_Log", "FPOrthMP"]
modeNums = [2, 3]

SchemaDict = {2: r"G:\My Drive\Scuba\test datasets\other testing\Python Experiments\Schema_2Modes.csv",
                3: r"G:\My Drive\Scuba\test datasets\other testing\Python Experiments\Schema_3Modes.csv"}

LevelDictFiles = {2: r"G:\My Drive\Scuba\test datasets\other testing\Python Experiments\Levels_2Modes.csv",
                3: r"G:\My Drive\Scuba\test datasets\other testing\Python Experiments\Levels_3Modes.csv"}

DescDeLim = ":: "
openVal = "(Open)"
nextDate = "1/1/2020"

outPath = r"C:\Users\david\OneDrive\Desktop\simOut.csv"
tempLoc = r"Z:\TEMP"

# Create random number generator, use seed
RNG = numpy.random.default_rng(2022)

############

# Create base starting points
baseDataDict = {
    2: pandas.read_csv(SchemaDict[2]).astype({"Units": "float64"}),
    3: pandas.read_csv(SchemaDict[3]).astype({"Units": "float64"}),
}
LevelDict = {2: pandas.read_csv(LevelDictFiles[2]), 3: pandas.read_csv(LevelDictFiles[3])}

# Simulation Module
@timeout.timeout(10)
def runSimulation(numModes : int, numCauses : int) -> dict:
    theSettingDict = {
        "SelectedStartDateText": "2019-01-01",
        "SelectedEndDateText": "2020-01-01",
        "DateFieldName": "Date",
        "RollupSelection": "",
        "DataFieldName": "Units",
        "dataUnits": "Units",
        "CSVFilePath": "",
        "HierLabels": ["H_A", "H_B"],
        "FullHierTable": [
            ["A", "AA", "", "", ""],
            ["B", "BB", "", "", ""],
            ["", "", "", "", ""],
            ["", "", "", "", ""],
            ["", "", "", "", ""]
        ],
        "SolverMethodToUse": "",
        "CSVOutputType": "Legacy",
        "OutFilePath": ""
    }

    temp_in_csv = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv", dir=tempLoc)
    temp_out_csv = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv", dir=tempLoc)
    temp_json = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json", dir=tempLoc)

    temp_in_csv.close()
    temp_out_csv.close()
    temp_json.close()

    # Create Individual Run Data and Settings, Run FactorPrism, Tabulate Results
    try:
        # Create starting data
        df = baseDataDict[numModes].copy()
        df["Units"] = RNG.normal(startMean, startSD, size=len(df))

        df_next = df.copy()
        df_next["Date"] = nextDate
        df_next["Units"] = (df_next["Units"] * (1 + RNG.normal(0, noiseSD, size=len(df_next)))).clip(lower=0)

        # Create effects, keep track of "actuals"
        leveldf = LevelDict[numModes]
        levelColumns = leveldf.columns[:4]
        causeIndices = RNG.choice(len(leveldf), size=numCauses)

        impactsdf = leveldf.copy()
        impactsdf["Impact"] = 0.0

        for ind in numpy.atleast_1d(causeIndices):
            causeLevel = leveldf.loc[ind]
            causeImpactPct = RNG.normal(0, effectSD)

            match_mask = pandas.Series(True, index=df_next.index)
            for col in levelColumns:
                val = causeLevel[col]
                if val != openVal:
                    match_mask &= df_next[col] == val

            if not match_mask.any():
                continue

            per_row_effect = RNG.normal(
                causeImpactPct,
                abs(causeImpactPct) * effectTermSDPct,
                size=match_mask.sum(),
            )

            current_units = df_next.loc[match_mask, "Units"]
            new_units = (current_units * (1 + per_row_effect)).clip(lower=0)
            df_next.loc[match_mask, "Units"] = new_units
            impactsdf.loc[ind, "Impact"] = (new_units - current_units).sum()

        df = pandas.concat([df, df_next], axis=0)
        df.to_csv(temp_in_csv.name, index=False)

        # Try for each solver method
        outDict = {}
        for solverMethod in solverMethods:
            theSettingDict["CSVFilePath"] = r"" + temp_in_csv.name
            theSettingDict["SolverMethodToUse"] = solverMethod
            theSettingDict["OutFilePath"] = r"" + temp_out_csv.name
            with open(temp_json.name, mode="w+", encoding="utf-8") as temp_json_file:
                json.dump(theSettingDict, temp_json_file)

            subprocess.run([FPConsolePath, temp_json.name], check=True)

            gc.collect()
            outDict[solverMethod] = score_result(temp_out_csv.name, impactsdf)

        return outDict
    finally:
        for temp_path in (temp_in_csv.name, temp_out_csv.name, temp_json.name):
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass




def score_result(resultCSVpath : str, trueImpactDF : pandas.DataFrame) -> float:
    resultDF = pandas.read_csv(resultCSVpath)
    totalAbsImpact = trueImpactDF["Impact"].abs().sum()

    def build_desc(row: pandas.Series) -> str:
        parts = [part for part in row.iloc[:4] if part != openVal]
        return DescDeLim.join(parts) if parts else "Overall"

    true_impact_with_desc = trueImpactDF.copy()
    true_impact_with_desc["Description"] = trueImpactDF.apply(build_desc, axis=1)

    net_by_desc = resultDF.groupby("Description")["Net Impact"].sum()
    result_impacts = true_impact_with_desc["Description"].map(net_by_desc).fillna(0)

    aligned = (
        (true_impact_with_desc["Impact"] != 0)
        & (result_impacts != 0)
        & (numpy.sign(true_impact_with_desc["Impact"]) == numpy.sign(result_impacts))
    )

    capturedAbsImpact = numpy.where(
        aligned,
        numpy.minimum(true_impact_with_desc["Impact"].abs(), result_impacts.abs()),
        0,
    ).sum()

    if totalAbsImpact == 0:
        return 0
    return capturedAbsImpact / totalAbsImpact

# Run simulation over different parameters
totalRuns = numRunsPerSetting * len(modeNums) * len(causesPerRun) * len(solverMethods)
run_index = 0
results = []
for _ in range(numRunsPerSetting):
    for mn in modeNums:
        for c in causesPerRun:
            print("----- Run " + str(run_index + 1) + " of " + str(totalRuns) + ".")
            try:
                simResult = runSimulation(mn, c)
                for meth in solverMethods:
                    results.append(
                        {
                            "numModes": mn,
                            "numCauses": c,
                            "method": meth,
                            "accuracy": simResult[meth],
                        }
                    )
                    run_index += 1
            except Exception as e:
                # Log the error details
                print(f"Error occurred during run {run_index + 1} (numModes={mn}, numCauses={c}):")
                print(f"{type(e).__name__}: {e}")
                # Print a detailed traceback for debugging
                traceback.print_exc()
                time.sleep(5)  # Allow some time for database or other processes to settle
                continue

out_df = pandas.DataFrame(results, columns=["numModes", "numCauses", "method", "accuracy"])
out_df.to_csv(outPath, index=False)
