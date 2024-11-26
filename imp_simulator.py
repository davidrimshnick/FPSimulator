# Simulate Hidden Causes
# Measure FP ability to test versus greedy algorithms

from asyncio.subprocess import DEVNULL
from cgitb import reset
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
from shutil import copyfile

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
solverMethods = ["GreedyTopDown", "GreedyBottomUp", "FPLP", "FPIteratedRegression"]
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
        "CSVOutputType": "Legacy",
        "OutFilePath": ""
    }

    temp_in_csv = tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".csv", dir=tempLoc)
    temp_in_csv.close()
    temp_out_csv = tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".csv", dir=tempLoc)
    temp_out_csv.close()
    temp_json = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json", dir=tempLoc)

    # Create Individual Run Data and Settings, Run FactorPrism, Tabulate Results

    # Create starting data
    df = baseDataDict[numModes].copy()
    for i in range(len(df)):
        df.loc[i, "Units"] = RNG.normal(startMean, startSD)
    df_next = df.copy()
    df_next["Date"] = nextDate
    for i in range(len(df_next)):
        df_next.loc[i, "Units"] *= (1 + RNG.normal(0, noiseSD))
        df_next.loc[i, "Units"] = max(0, pandas.to_numeric(df_next.loc[i, "Units"]))

    # Create effects, keep track of "actuals"
    leveldf = LevelDict[numModes]
    causeIndices = RNG.choice(len(leveldf), size=numCauses)

    impactsdf = leveldf.copy()
    impactsdf["Impact"] = 0.0

    for ind in causeIndices:
        causeLevel = leveldf.loc[[ind]]
        causeImpactPct = RNG.normal(0, effectSD)
        totalCauseImpact = 0.0

        for i in range(len(df_next)):
            if rowMatch(causeLevel, df_next.loc[[i]]):
                # base_impact = df_next.loc[i, "Units"] * causeImpactPct
                base_impact = df_next.loc[i, "Units"] * RNG.normal(causeImpactPct, abs(causeImpactPct) * effectTermSDPct)
                newVal = max(0, pandas.to_numeric(df_next.loc[i, "Units"]) + base_impact)
                impact = newVal - df_next.loc[i, "Units"] # in case it was truncated
                df_next.loc[i, "Units"] =  newVal
                totalCauseImpact += impact

        impactsdf.loc[ind, "Impact"] = totalCauseImpact

    #df = df.append(df_next)
    df = pandas.concat([df,df_next], axis=0)
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

        gc.collect()
        outDict[solverMethod] = score_result(temp_out_csv.name, impactsdf)
        
        # Debugging
        #copyfile(temp_out_csv.name, (r"Z:\Temp\out_" + solverMethod +".csv"))        

    # Debugging
    #impactsdf.to_csv(r"Z:\Temp\realimpacts.csv")



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
            if trueImpactDF.iloc[i, j] != openVal:
                if desc != "":
                    desc += DescDeLim
                desc += trueImpactDF.iloc[i, j]
        if (desc==""):
            desc="Overall"

        trueImpact = trueImpactDF.loc[i, "Impact"].sum()
        thisImpact = resultDF[resultDF["Description"]==desc]["Net Impact"].sum() # sum() is cheap to_numeric
        if trueImpact != 0 and thisImpact !=0 and abs(trueImpact)/trueImpact == abs(thisImpact)/thisImpact: # need to go in same direction
            capturedAbsImpact += min(abs(trueImpact), abs(thisImpact))        

    if totalAbsImpact == 0:
        return 0  
    return capturedAbsImpact / totalAbsImpact



def rowMatch(levelRow: pandas.DataFrame, matchRow: pandas.DataFrame) -> bool:
    for col in range(4):
        if (levelRow.iloc[0, col] != openVal and levelRow.iloc[0, col] != matchRow.iloc[0, col]):
            return False
    return True


# Run simulation over different parameters
totalRuns = numRunsPerSetting * len(modeNums) * len(causesPerRun) * len(solverMethods)
i=0
out_df = pandas.DataFrame(columns=["numModes", "numCauses", "method", "accuracy"])
while i < totalRuns:
    for mn in modeNums:
        for c in causesPerRun:
            print("----- Run " + str(i+1) + " of " + str(totalRuns) + ".")
            try:
                simResult = runSimulation(mn, c)
                for meth in solverMethods:
                    out_df.loc[i,"numModes"] = mn
                    out_df.loc[i,"numCauses"] = c
                    out_df.loc[i,"method"] = meth
                    out_df.loc[i,"accuracy"] = simResult[meth]
                    i=i+1
            except Exception as e:
                # Log the error details
                print(f"Error occurred during run {i+1} (numModes={mn}, numCauses={c}):")
                print(f"{type(e).__name__}: {e}")
                # Print a detailed traceback for debugging
                traceback.print_exc()
                time.sleep(5)  # Allow some time for database or other processes to settle
                continue

out_df.to_csv(outPath)