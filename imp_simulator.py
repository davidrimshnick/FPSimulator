# Simulate Hidden Causes
# Measure FP ability to test versus greedy algorithms

import json
import tempfile
import os
import subprocess
import pandas
import numpy

########
### Paramaters

FPConsolePath = r"C:\Users\david\source\repos\FactorPrismGit\FactorPrismDesktopWF\FPConsole\bin\x64\Debug\net48\FactorPrismConsole.exe"

startMean = 10000
startSD = 2000
effectSD = .01 # effect size small so interaction terms don't matter; centered at 0 and percent of original value as SD
noiseSD = .001

causesPerRun = [1, 3, 5]
numRunsPerSetting = 1000
solverMethods = ["GreedyTopDown", "GreedyBottomUp", "FPLP"]
modeNums = [2, 3]

SchemaDict = {2: r"C:\Users\david\Google Drive\Scuba\test datasets\other testing\Python Experiments\Schema_2Modes.csv",
                3: r"C:\Users\david\Google Drive\Scuba\test datasets\other testing\Python Experiments\Schema_3Modes.csv"}

LevelDict = {2: r"C:\Users\david\Google Drive\Scuba\test datasets\other testing\Python Experiments\Levels_2Modes.csv",
                3: r"C:\Users\david\Google Drive\Scuba\test datasets\other testing\Python Experiments\Levels_3Modes.csv"}

openVal = "(Open)"
nextDate = "1/1/2020"

outPath = r"C:\Users\david\Desktop\simOut.csv"

############

# Create base starting points
baseDataDict = {2: pandas.read_csv(SchemaDict[2]), 3: pandas.read_csv(SchemaDict[3])}
LevelDict = {2: pandas.read_csv(LevelDict[2]), 3: pandas.read_csv(LevelDict[3])}

# Simulation Module
def runSimulation(numModes : int, numCauses : int) -> dict:
    # Create base settings dictionary, to be edited on each run
    theSettingDict =  {
        "SelectedStartDateText": "2019-01-01",
        "SelectedEndDateText": "2020-01-01",
        "DateFieldName": "Date",
        "RollupSelection": "",
        "DataFieldName": "Units",
        "dataUnits": "Units",
        "CSVFilePath": "",
        "HierLabels": [ "H_A", "H_B"],
        "FullHierTable": [
            [ "A", "AA", "", "", "" ],
            [ "B", "BB", "", "", "" ],
            [ "", "", "", "", "" ],
            [ "", "", "", "", "" ],
            [ "", "", "", "", "" ]
        ],
        "IsStaticAnalysis": False,
        "SolverMethodToUse": "",
        "FactorCSVOutputOverride": True,
        "OutFilePath": ""
    }

    temp_in_csv = tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".csv")
    temp_in_csv.close()
    temp_out_csv = tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".csv")
    temp_out_csv.close()
    temp_json = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json")

    # Create Individual Run Data and Settings, Run FactorPrism, Tabulate Results

    # Create starting data
    df = baseDataDict[numModes].copy()
    for i in range(len(df)):
        df.loc[i, "Units"] = numpy.random.normal(startMean, startSD)
    df_next = df.copy()
    df_next["Date"] = nextDate
    for i in range(len(df_next)):
        df_next.loc[i, "Units"] *= (1 + numpy.random.normal(0, noiseSD))
        df_next.loc[i, "Units"] = max(0, pandas.to_numeric(df_next.loc[i, "Units"]))

    # Create effects, keep track of "actuals"
    leveldf = LevelDict[numModes]
    causeIndices = numpy.random.choice(len(leveldf), size=numCauses)

    impactsdf = leveldf.copy()
    impactsdf["Impact"] = 0.0

    for ind in causeIndices:
        causeLevel = leveldf.loc[[ind]]
        causeImpactPct = numpy.random.normal(0, effectSD)

        for i in range(len(df_next)):
            if rowMatch(causeLevel, df_next.loc[[i]]):
                impact = df_next.loc[i, "Units"] * causeImpactPct
                df_next.loc[i, "Units"] = max(0, pandas.to_numeric(df_next.loc[i, "Units"]) + impact)
                impactsdf.loc[i, "Impact"] += impact

    df = df.append(df_next)
    df.to_csv(temp_in_csv.name, index=False)

    # Try for each solver method
    outDict = {}
    for solverMethod in solverMethods:
        theSettingDict["CSVFilePath"] = r"" + temp_in_csv.name
        theSettingDict["SolverMethodToUse"] = solverMethod
        theSettingDict["OutFilePath"] = r"" + temp_out_csv.name
        temp_json = open(temp_json.name, mode="w+")
        json.dump(theSettingDict, temp_json)
        temp_json.close()
        subprocess.run(FPConsolePath + " " + temp_json.name)
        outDict[solverMethod] = score_result(temp_out_csv.name, impactsdf)

    # clean up files
    temp_json.close()
    os.unlink(temp_in_csv.name)
    os.unlink(temp_out_csv.name)
    os.unlink(temp_json.name)

    return outDict



def score_result(resultCSVpath : str, trueImpactDF : pandas.DataFrame) -> float:
    resultDF = pandas.read_csv(resultCSVpath)
    totalAbsImpact = trueImpactDF["Impact"].abs().sum()

    capturedAbsImpact = 0.0
    for i in range(len(trueImpactDF)):
        # Make description string to match whats in FP output
        desc = ""
        for j in range(4): # exclude last column because its the impact one
            if trueImpactDF.iloc[0, j] != openVal:
                if desc != "":
                    desc += " - "
                desc += trueImpactDF.iloc[i, j]
        if (desc==""):
            desc="Overall"

        trueImpact = trueImpactDF.loc[i, "Impact"].sum()
        thisImpact = resultDF[resultDF["Description"]==desc]["Net Impact"].sum() # sum() is cheap to_numeric
        if trueImpact != 0 and thisImpact !=0 and abs(trueImpact)/trueImpact == abs(thisImpact)/thisImpact: # need to go in same direction
            capturedAbsImpact += min(abs(trueImpact), abs(thisImpact))

    return capturedAbsImpact / totalAbsImpact


def rowMatch(levelRow: pandas.DataFrame, matchRow: pandas.DataFrame) -> bool:
    for col in range(4):
        if (levelRow.iloc[0, col] != openVal and levelRow.iloc[0, col] != matchRow.iloc[0, col]):
            return False
    return True


# Run simulation over different parameters
i=0
out_df = pandas.DataFrame(columns=["numModes", "numCauses", "method", "accuracy"])
for r in range(numRunsPerSetting):
    for mn in modeNums:
        for c in causesPerRun:
            simResult = runSimulation(mn, c)
            for meth in solverMethods:
                out_df.loc[i,"numModes"] = mn
                out_df.loc[i,"numCauses"] = c
                out_df.loc[i,"method"] = meth
                out_df.loc[i,"accuracy"] = simResult[meth]
                i=i+1

out_df.to_csv(outPath)