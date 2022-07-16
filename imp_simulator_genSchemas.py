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
import pandas as pd
import random
import string
from shutil import copyfile
from datetime import datetime

########
def randString(numChars):
    return ''.join(random.choices(string.ascii_uppercase, k=numChars))

### Paramaters to alter

FPConsolePath = r"G:\My Drive\Scuba\Publish Location\XPConsoleProfiles\win-x64\FactorPrismConsoleXP.exe"

startMean = 10000
startSD = 2000
effectSD = .05 # effect size small so interaction terms don't matter; centered at 0 and percent of original value as SD
effectTermSDPct = .1 # how big the effect differs in subcategories as percent of effect
noiseSD = .001

causesPerRun = [1, 3, 5]
numRunsPerSetting = 1
solverMethods = ["GreedyTopDown", "GreedyBottomUp", "FPLP", "FPIteratedRegression"]
modeNums = [2, 3] #5
Hier1Size = 2
Hier2Size = 2

fn = (
    datetime.now().strftime("%Y%m%d-%H%M%S") + "_runsper-" + str(numRunsPerSetting) +
        "_h1s-" + str(Hier1Size) + "_h2s-" + str(Hier2Size)
)
outPath = r"G:\My Drive\Scuba\test datasets\other testing\Python Experiments\rawOuts\simOut_" + fn + ".csv"

##### Shouldn't need to alter below
Hiers = [[randString(5) for x in range(Hier1Size)],[randString(5) for x in range(Hier1Size)]]
colNames = Hiers[0] + Hiers[1]
openVal = "(Open)"
startDate = "1/1/2019"
nextDate = "1/1/2020"
randFieldSize = 10
tempLoc = r"Z:\TEMP"

# Create random number generator, use seed
RNG = numpy.random.default_rng(2022)
############


### Helpers to create JSON

def fillBlankList(partList, listLen):
    outList = [""] * listLen
    for i in range(len(partList)):
        outList[i] = partList[i]
    return outList



baseDataDict = {}
LevelDict = {}

def createBaseData():

    for m in modeNums:

        def DictListCross(dict1list : list, dict2list : list):
            outDictList = []
            for d1 in dict1list:
                for d2 in dict2list:
                    outDictLine = d1 | d2
                    outDictList.append(outDictLine)
            return outDictList

        def makeNewRow(curRow, curIndex):
            HierLevelRows.append(curRow.copy())

            for i in range(m):
                newRow = curRow.copy()
                newRow[h[curIndex]] = randString(randFieldSize)

                if (curIndex==(len(h)-1)):
                    HierLevelRows.append(newRow.copy())
                    HierDataRows.append(newRow.copy())
                else:
                    makeNewRow(newRow, curIndex+1)

        # make recurrent call to make these rows

        HiersLevelRows = {}
        HiersDataRows = {}

        for hnum in range(2):
            h = Hiers[hnum]
            HierLevelRows = []
            HierDataRows = []
            startRow = dict.fromkeys(h, openVal)
            makeNewRow(startRow,0)

            HiersLevelRows[hnum] = HierLevelRows
            HiersDataRows[hnum] = HierDataRows

        baseDataRows = DictListCross(HiersDataRows[0], HiersDataRows[1])
        levelRows = DictListCross(HiersLevelRows[0], HiersLevelRows[1])


        # create dataframes from list of dicts (https://www.geeksforgeeks.org/create-a-pandas-dataframe-from-list-of-dicts/)

        startDataDict = pd.DataFrame(baseDataRows)
        startDataDict["Units"] = 0
        startDataDict["Date"] = startDate
        baseDataDict[m] = startDataDict
        LevelDict[m] = pd.DataFrame(levelRows)






# Simulation Module
@timeout.timeout(200)
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
        "HierLabels": [randString(5) for x in range(len(Hiers))],
        "FullHierTable": [
            fillBlankList(Hiers[0], 5),
            fillBlankList(Hiers[1], 5),
            [ "", "", "", "", "" ],
            [ "", "", "", "", "" ],
            [ "", "", "", "", "" ]
        ],
        "IsStaticAnalysis": False,
        "SolverMethodToUse": "",
        "FactorCSVOutputOverride": True,
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
        # copyfile(temp_out_csv.name, (r"Y:\Temp\out_" + solverMethod +".csv"))

    # Debugging
    # impactsdf.to_csv(r"Y:\Temp\realimpacts.csv")



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
totalRuns = numRunsPerSetting * len(modeNums) * len(causesPerRun) * len(solverMethods)
i=0
out_df = pandas.DataFrame(columns=["numModes", "numCauses", "method", "accuracy"])
createBaseData()
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
            except:
                time.sleep(5) # to give database time to settle down
                continue

out_df.to_csv(outPath)