import pymssql
import pandas as pd
import logging
import datetime
from dailyEmail import daily_email
import json
import os


# Set Status of File
Stage = os.environ.get("PYENV_HOME")
print(Stage)

# Open config.json file and load data
with open("config.json") as json_data_file:
    my_config = json.load(json_data_file)

# Determine status and access values for correct config file
if Stage == "DEV":
    config = my_config["uat"]
else:
    config = my_config["prod"]

# Create Logging File with configs
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="dailykit_log.log", level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger()

# Create Connection to Database
cnxn = pymssql.connect(
    config["Server"], config["uid"], config["password"], config["Database"]
)

# Create a list of OnHand Finished Product
QtyOnHand = pd.read_sql(
    "Select * FROM BenchmadeDB.dbo.VU_DailyKitTopSKUQtyOnHand", cnxn
)
QtyOnHand_OG = QtyOnHand.copy(deep=True)

# Create a list of Priority items
priority_items = pd.read_sql(
    "Select * FROM BenchmadeDB.dbo.VU_DailyKitPriorityList", cnxn
)
priority_items = priority_items.sort_values("Priority")
priority_items = priority_items.reset_index()
priority_items.drop(labels=["index"], axis=1, inplace=True)

# Create List of items
items_p = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitItemsP", cnxn)

# Create List of active skus
active_skus = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitActiveSKU", cnxn)
active_skus_max = pd.read_sql(
    "Select * From BenchmadeDB.dbo.VU_DailyKitActiveSKU", cnxn
)

# Reduce list of need to make based on what is avilable in the warehouse
logging.info("Creating new Priority_Items List")
for index, row in priority_items.iterrows():
    try:
        QtyOnHand_item = QtyOnHand.loc[
            QtyOnHand["StockCode"] == row["StockCode"], "TotalOnHand"
        ].item()
        if QtyOnHand_item >= row["Total"]:
            QtyOnHand.loc[
                QtyOnHand["StockCode"] == row["StockCode"], "TotalOnHand"
            ] -= row["Total"]
            priority_items.iat[index, 2] = 0
        else:
            priority_items.iat[index, 2] -= QtyOnHand_item
            QtyOnHand.at[QtyOnHand["StockCode"] == row["StockCode"], "TotalOnHand"] = 0
    except:
        logging.error(
            "{} has none on hand in warehouses F, G or H".format(row["StockCode"])
        )
logging.info("New Prioirty List Completed.")

# Create List of All Parent Components
bom_list = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitCompList", cnxn)

# Find BOM Structure
def bomStructure(stockCode):
    global bom_list
    comp_list = bom_list.query("ParentPart == '{}'".format(stockCode))

    return comp_list


# Creates dict of Components and min one
def leastOfThese(comp_list, items_list):

    n = 100000
    c = ""

    for comp in comp_list["Component"].unique():
        y = (
            items_list.loc[items_list["StockCode"] == comp, "QtyOnHand"].item()
            // comp_list.loc[comp_list["Component"] == comp, "QtyPer"].item()
        )
        if y < n:
            n = y
            c = comp

    return (n, c)


# Loop though all active skus, build dataframe of available to kit and save to csv
def main():

    kit_items = pd.DataFrame(
        columns=[
            "StockCode",
            "LimitComponent",
            "Description",
            "BuildQty",
            "QtyNeeded",
            # "QtyOrdered", # Simplifying Visability
            "InWIP",
            # "QtyOnHand", # Simplifying Visability
            "Priority",
            "MakeBuy",
        ]
    )

    # Loop for priority items first
    for index, row in priority_items.iterrows():
        item = row["StockCode"]

        comp_list = bomStructure(item)

        try:
            n, c = leastOfThese(comp_list, items_p)

            if n >= 10 and n > row["Total"]:
                buildqty = (row["Total"] // 10) * 10

            elif n >= 10 and n < row["Total"]:
                buildqty = (n // 10) * 10

            else:
                buildqty = 0

            if buildqty > 0:
                for comp in comp_list["Component"].unique():
                    try:
                        items_p.loc[items_p["StockCode"] == comp, "QtyOnHand"] -= (
                            buildqty
                            * comp_list.loc[
                                comp_list["Component"] == comp, "QtyPer"
                            ].item()
                        )

                    except Exception as e:
                        logger.error("{} : {}".format(item, e))

                kit_items = kit_items.append(
                    {
                        "StockCode": item,
                        "LimitComponent": c,
                        "Description": items_p.loc[
                            items_p["StockCode"] == c, "Description"
                        ].item(),
                        "BuildQty": buildqty,
                        "QtyNeeded": row["Total"],
                        # "QtyOrdered": row["TotalOrdered"], # Simplifying Visability
                        "InWIP": row["OnOrder"],
                        # "QtyOnHand": QtyOnHand_OG.loc[
                        #     QtyOnHand_OG["StockCode"] == row["StockCode"], "TotalOnHand"
                        # ].item(), # Simplifying Visability
                        "Priority": row["Priority"],
                        "MakeBuy": items_p.loc[
                            items_p["StockCode"] == c, "PartCategory"
                        ].item(),
                    },
                    ignore_index=True,
                )
        except Exception as e:

            logger.error("{} : {}".format(item, e))

    # Loop for items in active List
    for item in active_skus["StockCode"].unique():
        # Run bomStructure Function for each StockCode
        comp_list = bomStructure(item)

        try:
            n, c = leastOfThese(comp_list, items_p)

            if n >= 10:
                for comp in comp_list["Component"].unique():
                    buildqty = n // 10 * 10
                    try:
                        items_p.loc[items_p["StockCode"] == comp, "QtyOnHand"] -= (
                            buildqty
                            * comp_list.loc[
                                comp_list["Component"] == comp, "QtyPer"
                            ].item()
                        )

                    except Exception as e:
                        logger.error("{} : {}".format(item, e))

                kit_items = kit_items.append(
                    {
                        "StockCode": item,
                        "LimitComponent": c,
                        "Description": items_p.loc[
                            items_p["StockCode"] == c, "Description"
                        ].item(),
                        "BuildQty": buildqty,
                        "MakeBuy": items_p.loc[
                            items_p["StockCode"] == c, "PartCategory"
                        ].item(),
                    },
                    ignore_index=True,
                )

        except Exception as e:

            logger.error("{} : {}".format(item, e))

    filename = (
        "DailyKittable"
        + str(datetime.datetime.now())
        .replace(".", " ")
        .replace(":", "_")
        .replace(" ", "_")
        + ".xlsx"
    )
    with pd.ExcelWriter(
        filename,
        engine="xlsxwriter",
    ) as writer:
        kit_items.to_excel(writer, sheet_name="kittable", index=False)

    logger.info("DailyKittable.xlsx file has been created.")

    try:
        daily_email(filename)

    except Exception as e:

        logger.critical("FAILED TO SEND EMAIL : {}".format(e))


if __name__ == "__main__":
    try:
        logger.info("Running dailyKittable_live.py file")
        main()
        logger.info("Closing Connection to database")
        cnxn.close()
    except Exception as e:
        logging.critical("FAILLED TO EXECUTE : {}".format(e))
