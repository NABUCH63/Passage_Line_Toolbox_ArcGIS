# -*- coding: utf-8 -*-

# import dependencies and libraries
import pandas as pd
import math
import arcpy

## Define parameter inputs, outputs
TrackLines = str(arcpy.GetParameterAsText(0))
PassageLines = str(arcpy.GetParameterAsText(1))
MultipleInt = str(arcpy.GetParameterAsText(2))
NameField = str(arcpy.GetParameterAsText(3))
MMSI = str(arcpy.GetParameterAsText(4))
LengthField = str(arcpy.GetParameterAsText(5))
VesselTypes = str(arcpy.GetParameterAsText(6))
Simplified = str(arcpy.GetParameterAsText(7))
CustomName = str(arcpy.GetParameterAsText(8))
if arcpy.GetParameterAsText(9):
    CustomTypes = [int(num) for num in arcpy.GetParameterAsText(9).split(";")]
else:
    CustomTypes = arcpy.GetParameterAsText(9)
DateField = str(arcpy.GetParameterAsText(10))
StartInterval = str(arcpy.GetParameterAsText(11))
EndInterval = str(arcpy.GetParameterAsText(12))
OutputName = str(arcpy.GetParameterAsText(13))

## Spatial Join tool execution
def PassageLineSummary(TrackLines, PassageLines, MultipleInt, NameField, VesselTypes, Simplified, CustomName, CustomTypes, DateField, StartInterval, EndInterval, OutputName):
    # define temporary output for spatial join
    temp_out = "temp_spatial_join"
    # execute join or intersect

    ## count the number of input features
    counterp = 0
    countert = 0
    for row in arcpy.SearchCursor(PassageLines, arcpy.ListFields(PassageLines)):
        counterp += 1
    for row in arcpy.SearchCursor(TrackLines, arcpy.ListFields(TrackLines)):
        countert += 1
    arcpy.AddMessage(f"Passage line input: {counterp} items...")
    arcpy.AddMessage(f"Track line input: {countert} items...")
    arcpy.AddMessage("Running Pairwise Intersect...")

    ## Describe the shapetype of the passage line/area input
    descp = arcpy.Describe(PassageLines)

    ## convert to polyline if polygon, and get multipoint output
    if descp.shapeType == "Polygon":
        arcpy.management.PolygonToLine(in_features=PassageLines, out_feature_class="Temp_PassageLines", neighbor_option="IGNORE_NEIGHBORS")
        arcpy.analysis.PairwiseIntersect(in_features=[TrackLines, "Temp_PassageLines"], out_feature_class=temp_out, join_attributes="ALL", cluster_tolerance="", output_type="POINT")
    elif descp.shapeType == "Line" or descp.shapeType == "Polyline":
        arcpy.analysis.PairwiseIntersect(in_features=[TrackLines, PassageLines], out_feature_class=temp_out, join_attributes="ALL", cluster_tolerance="", output_type="POINT")
    ## multipart to single part, convert pairwise intersect output to individual points
    arcpy.management.MultipartToSinglepart(in_features=temp_out, out_feature_class="temp_int")

    ## count the number of multipoint and singlepoint features
    countero = 0
    for row in arcpy.SearchCursor("temp_int", arcpy.ListFields("temp_int")):
        countero += 1
    counteri = 0
    for row in arcpy.SearchCursor(temp_out, arcpy.ListFields(temp_out)):
        counteri += 1

    arcpy.AddMessage(f"Pairwise Intersect multipart feature class output: {counteri} items...")
    arcpy.AddMessage(f"Pairwise Intersect converted to singlepart feature class output: {countero} items...")

## Convert singlepoint output to dataframe
    final_fields = [field.name for field in arcpy.ListFields("temp_int")]
    data = [row for row in arcpy.da.SearchCursor("temp_int", final_fields)]
    temp_int_df = pd.DataFrame(data, columns=final_fields)
    temp_orig_fid = temp_int_df.groupby(by="ORIG_FID").size()
## Convert multipoint output to dataframe
    final_fields1 = [field.name for field in arcpy.ListFields(temp_out)]
    data1 = [row for row in arcpy.da.SearchCursor(temp_out, final_fields1)]
    temp_out_df = pd.DataFrame(data1, columns=final_fields1)
    temp_out_df["ORIG_FID_CALC"] = 0

## Determine the number of interactions per multipoint feature. If a polygon, the total interactions per feature will be half
## rounded to next highest integer. (if a vessel passes through an area, it will create two interactions that represent one transit.)
    for i in temp_out_df.index:
        if MultipleInt == "true":
            if descp.shapeType == "Polygon":
                temp_out_df.loc[i, "ORIG_FID_CALC"] = math.ceil(temp_orig_fid[temp_out_df["OBJECTID"][i]] / 2)
            else:
                temp_out_df.loc[i, "ORIG_FID_CALC"] = temp_orig_fid[temp_out_df["OBJECTID"][i]]
        else:
            temp_out_df.loc[i, "ORIG_FID_CALC"] = 1

    arcpy.AddMessage(f"Found {temp_out_df['ORIG_FID_CALC'].sum()} interactions...")

## If a date/interval was given, select only those entries. Else, select the whole dataset.
    if pd.isnull(StartInterval) == True:
        StartInterval = ""
    if pd.isnull(EndInterval) == True:
        EndInterval = ""
    if DateField != "" and StartInterval == "" and EndInterval == "":
        arcpy.AddMessage("No valid interval input, calculating for the entire dataset...")
    if DateField != "" and StartInterval != "" and EndInterval != "":
        temp_out_df = [(temp_out_df[DateField] >= pd.to_datetime(StartInterval)) & (temp_out_df[DateField] <= pd.to_datetime(EndInterval))]
        arcpy.AddMessage(f"Running summary for data within the interval {StartInterval} to {EndInterval}...")

## find transit and unique counts
    transit_df = temp_out_df[[NameField, VesselTypes, "ORIG_FID_CALC"]].groupby(by=[NameField, VesselTypes]).sum().reset_index()
    unique_df = temp_out_df.groupby(by=[NameField, VesselTypes, MMSI]).count().reset_index()
## create new dataframe to summarize each vessel type for each waterway/passageline
    new_df = pd.DataFrame(columns=["Name", "Vessel_Type", "Total_Transits", "Unique_Vessels"])
    temp_type = pd.unique(temp_out_df[VesselTypes])
    temp_label = pd.unique(temp_out_df[NameField])

## Populate the new dataframe with waterway/passageline names and vesseltypes
    for x in range(len(temp_label)):
        for i in range(len(temp_type)):
            new_df = new_df.append({"Name": temp_label[x], "Vessel_Type": temp_type[i]}, ignore_index=True)

    arcpy.AddMessage("Creating summary dataframe...")

## Populate the transit and unique counts in the new dataframe
    for y in new_df.index:
        temp_transit = transit_df[(transit_df[VesselTypes] == new_df["Vessel_Type"][y]) & (transit_df[NameField] == new_df["Name"][y])].reset_index(drop=True)
        if len(temp_transit) > 0:
            new_df.loc[y, "Total_Transits"] = temp_transit["ORIG_FID_CALC"][0]
        else:
            new_df.loc[y, "Total_Transits"] = 0
        temp_unique = unique_df[(unique_df[VesselTypes] == new_df["Vessel_Type"][y]) & (unique_df[NameField] == new_df["Name"][y])].reset_index(drop=True)
        if len(temp_unique) > 0:
            new_df.loc[y, "Unique_Vessels"] = len(temp_unique[MMSI])
        else:
            new_df.loc[y, "Unique_Vessels"] = 0

    arcpy.AddMessage("Populating summary dataframe...")

## get list of waterway/passageline names for final summary dataframe
    Aggregate_temp = new_df.groupby(by="Name").sum().reset_index()
    Aggregate_temp = Aggregate_temp["Name"]

## check if a simplified table was selected
    if Simplified == "true":

        arcpy.AddMessage("Simplified dataframe selected...")

    ## Create simplified final summary dataframe
        if CustomName and CustomTypes:
            Passage_line_df = pd.DataFrame(
                columns=["Name", "Max_Len", "Avg_Len", "Unique_Fishing", "Transits_Fishing", "Unique_Tug_Tow", "Transits_Tug_Tow",
                         "Unique_Recreational", "Transits_Recreational", "Unique_Mil_LE", "Transits_Mil_LE", "Unique_Pilots", "Transits_Pilots", "Unique_Passenger", "Transits_Passenger",
                         "Unique_Cargo", "Transits_Cargo", "Unique_Tanker", "Transits_Tanker", f"Unique_{CustomName}", f"Transits_{CustomName}", "Unique_Other", "Transits_Other",
                         "Unique_Total", "Transits_Total"])
        else:
            Passage_line_df = pd.DataFrame(
                columns=["Name", "Max_Len", "Avg_Len", "Unique_Fishing", "Transits_Fishing", "Unique_Tug_Tow",
                         "Transits_Tug_Tow",
                         "Unique_Recreational", "Transits_Recreational", "Unique_Mil_LE", "Transits_Mil_LE",
                         "Unique_Pilots", "Transits_Pilots", "Unique_Passenger", "Transits_Passenger",
                         "Unique_Cargo", "Transits_Cargo", "Unique_Tanker", "Transits_Tanker", "Unique_Other", "Transits_Other",
                         "Unique_Total", "Transits_Total"])

        Passage_line_df["Name"] = Aggregate_temp
        Passage_line_df["Max_Len"] = 0
        Passage_line_df["Avg_Len"] = 0
        Passage_line_df["Unique_Fishing"] = 0
        Passage_line_df["Unique_Tug_Tow"] = 0
        Passage_line_df["Unique_Recreational"] = 0
        Passage_line_df["Unique_Pilots"] = 0
        Passage_line_df["Unique_Mil_LE"] = 0
        Passage_line_df["Unique_Passenger"] = 0
        Passage_line_df["Unique_Cargo"] = 0
        Passage_line_df["Unique_Tanker"] = 0
        Passage_line_df["Unique_Other"] = 0
        Passage_line_df["Unique_Total"] = 0
        Passage_line_df["Transits_Fishing"] = 0
        Passage_line_df["Transits_Tug_Tow"] = 0
        Passage_line_df["Transits_Recreational"] = 0
        Passage_line_df["Transits_Pilots"] = 0
        Passage_line_df["Transits_Mil_LE"] = 0
        Passage_line_df["Transits_Passenger"] = 0
        Passage_line_df["Transits_Cargo"] = 0
        Passage_line_df["Transits_Tanker"] = 0
        Passage_line_df["Transits_Other"] = 0
        Passage_line_df["Transits_Total"] = 0

        if CustomName and CustomTypes:
            Passage_line_df[f"Unique_{CustomName}"] = 0
            Passage_line_df[f"Transits_{CustomName}"] = 0

        arcpy.AddMessage("Creating aggregated summary dataframe...")

    ## populate the final dataframe
        for i in Passage_line_df.index:
            if LengthField:
                maxim = [x for x in temp_out_df[temp_out_df[NameField] == Passage_line_df["Name"][i]]["Length"] if math.isnan(x) == False]
            else: maxim = []
            if len(maxim) > 0:
                Passage_line_df.loc[i, "Max_Len"] = max(maxim)
                Passage_line_df.loc[i, "Avg_Len"] = round(sum(maxim)/len(maxim), 2)
            else:
                Passage_line_df.loc[i, "Max_Len"] = 0
                Passage_line_df.loc[i, "Avg_Len"] = 0
            for z in new_df.index:
                if new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Total"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Total"] += new_df["Total_Transits"][z]
                if new_df["Vessel_Type"][z] == 30 and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Fishing"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Fishing"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] in [31, 32, 52]) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Tug_Tow"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Tug_Tow"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] in [36, 37]) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Recreational"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Recreational"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] >= 60 and new_df["Vessel_Type"][z] <= 69) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Passenger"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Passenger"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] >= 70 and new_df["Vessel_Type"][z] <= 79) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Cargo"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Cargo"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] >= 80 and new_df["Vessel_Type"][z] <= 89) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Tanker"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Tanker"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] == 50) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Pilots"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Pilots"] += new_df["Total_Transits"][z]
                elif (new_df["Vessel_Type"][z] in [35, 55]) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Mil_LE"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, "Transits_Mil_LE"] += new_df["Total_Transits"][z]
                elif CustomName and CustomTypes and (new_df["Vessel_Type"][z] in CustomTypes) and new_df["Name"][z] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, f"Unique_{CustomName}"] += new_df["Unique_Vessels"][z]
                    Passage_line_df.loc[i, f"Transits_{CustomName}"] += new_df["Total_Transits"][z]

            Passage_line_df.loc[i, "Unique_Other"] = Passage_line_df["Unique_Total"][i]
            Passage_line_df.loc[i, "Transits_Other"] = Passage_line_df["Transits_Total"][i]

            for n in new_df.index:
                if new_df["Vessel_Type"][n] == 30 and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] in [31, 32, 52]) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] in [36, 37]) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] >= 60 and new_df["Vessel_Type"][n] <= 69) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] >= 70 and new_df["Vessel_Type"][n] <= 79) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] >= 80 and new_df["Vessel_Type"][n] <= 89) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] == 50) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif (new_df["Vessel_Type"][n] in[35, 55]) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]
                elif CustomName and CustomTypes and (new_df["Vessel_Type"][n] in CustomTypes) and new_df["Name"][n] == Passage_line_df["Name"][i]:
                    Passage_line_df.loc[i, "Unique_Other"] -= new_df["Unique_Vessels"][n]
                    Passage_line_df.loc[i, "Transits_Other"] -= new_df["Total_Transits"][n]

        arcpy.AddMessage("Populating aggregated summary dataframe...")
## Non-simplified output
    else:
        Passage_line_df = new_df
        arcpy.AddMessage("Populating non-aggregated summary dataframe...")


## output as table

    ## convert output name to ESRI friendly format (no characters, spaces, and starts with an alpha character.)
    temp_output = ""
    for char in OutputName:
        if char.isalnum() == True:
            temp_output = temp_output+char
    if temp_output[0].isnumeric() == True:
        temp_output = "A"+temp_output
    OutputName = temp_output

    ## create empty table
    arcpy.management.CreateTable(arcpy.env.workspace, OutputName)
    fields = [field for field in Passage_line_df.columns]
    datatype = []

    arcpy.AddMessage("Creating output table...")

    for i in range(len(fields)):
        if i==0:
            datatype.append("TEXT")
        else:
            datatype.append("LONG")

    ## add fields from final summary dataframe to table
    for i in range(len(fields)):
        arcpy.management.AddField(OutputName, fields[i], datatype[i])

    ## initialize insert cursor for all fields
    cursor = arcpy.da.InsertCursor(OutputName, fields)

    ## insert all rows in table
    for i in range(len(Passage_line_df)):
        row = Passage_line_df.loc[i, :]
        cursor.insertRow(row)

    arcpy.AddMessage("Populating output table... check the current working environment's geodatabase for output.")

    ## delete cursor and temp_out spatial join
    arcpy.Delete_management(temp_out)
    arcpy.Delete_management("Temp_PassageLines")
    arcpy.Delete_management("temp_int")

    del cursor,temp_out
    return

## executable check

if __name__ == '__main__':
    TrackLines = str(arcpy.GetParameterAsText(0))
    PassageLines = str(arcpy.GetParameterAsText(1))
    MultipleInt = str(arcpy.GetParameterAsText(2))
    NameField = str(arcpy.GetParameterAsText(3))
    MMSI = str(arcpy.GetParameterAsText(4))
    LengthField = str(arcpy.GetParameterAsText(5))
    VesselTypes = str(arcpy.GetParameterAsText(6))
    Simplified = str(arcpy.GetParameterAsText(7))
    CustomName = str(arcpy.GetParameterAsText(8))
    if arcpy.GetParameterAsText(9):
        CustomTypes = [int(num) for num in arcpy.GetParameterAsText(9).split(";")]
    else:
        CustomTypes = arcpy.GetParameterAsText(9)
    DateField = str(arcpy.GetParameterAsText(10))
    StartInterval = str(arcpy.GetParameterAsText(11))
    EndInterval = str(arcpy.GetParameterAsText(12))
    OutputName = str(arcpy.GetParameterAsText(13))

    PassageLineSummary(TrackLines, PassageLines, MultipleInt, NameField, VesselTypes, Simplified, CustomName, CustomTypes, DateField, StartInterval, EndInterval, OutputName)
