import pyodbc
import pandas as pd
import logging
from dailyEmail import daily_email
import json
import os


# Set Status of File
Stage = os.environ.get('PYENV_HOME')
print(Stage)

# Open config.json file and load data
with open('config.json') as json_data_file:
    my_config = json.load(json_data_file)

#Determine status and access values for correct config file
if Stage == 'DEV':
    config = my_config['uat']
else:
    config = my_config['prod']

# Create Logging File with configs
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="dailykit_log.log", level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger()

# Create Connection to Database
cnxn = pyodbc.connect(
    "DRIVER={SQL Server Native Client 11.0};"
    f"Server={config['Server']};"
    f"Port={config['Port']};"
    f"Database={config['Database']};"
    "Trusted_Connection=yes;"
    f"uid={config['uid']};"
    f"pwd={config['password']}"
)


# Create List of items
items_p = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitItemsP", cnxn)
items_p_max = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitItemsP", cnxn)

# Create List of active skus
active_skus = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitActiveSKU", cnxn)
active_skus_max = pd.read_sql("Select * From BenchmadeDB.dbo.VU_DailyKitActiveSKU", cnxn)

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

def maxKit():

    max_items = pd.DataFrame(
        columns=["StockCode", "LimitComponent", "Description", "CanMake", "MakeBuy"]
    )

    for item in active_skus_max["StockCode"].unique():
        # Run bomStructure Function for each StockCode
        comp_list = bomStructure(item)

        try:
            n, c = leastOfThese(comp_list, items_p_max)

            if n < 20:
                logger.info("{} had less then 20 available parts to kit.".format(item))

            else:
                for comp in comp_list["Component"].unique():
                    buildqty = n // 20 * 20
                    try:
                        items_p_max.loc[items_p_max["StockCode"] == comp, "QtyOnHand"] -= (
                            buildqty
                            * comp_list.loc[
                                comp_list["Component"] == comp, "QtyPer"
                            ].item()
                        )

                    except Exception as e:
                        logger.error(e)

                max_items = max_items.append(
                    {
                        "StockCode": item,
                        "LimitComponent": c,
                        "Description": items_p_max.loc[
                            items_p_max["StockCode"] == c, "Description"
                        ].item(),
                        "CanMake": buildqty,
                        "MakeBuy": items_p_max.loc[
                            items_p_max["StockCode"] == c, "PartCategory"
                        ].item(),
                    },
                    ignore_index=True,
                )
        except Exception as e:

            logger.error("{} : {}".format(item, e))

    return max_items


# Loop though all active skus, build dataframe of available to kit and save to csv
def main():

    kit_items = pd.DataFrame(
        columns=["StockCode", "LimitComponent", "Description", "CanMake", "MakeBuy"]
    )

    for item in active_skus["StockCode"].unique():
        # Run bomStructure Function for each StockCode
        comp_list = bomStructure(item)

        try:
            n, c = leastOfThese(comp_list, items_p)

            if n < 20:
                logger.info("{} had less then 20 available parts to kit.".format(item))

            elif n < 200:
                for comp in comp_list["Component"].unique():
                    buildqty = n // 20 * 20
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
                        "CanMake": buildqty,
                        "MakeBuy": items_p.loc[
                            items_p["StockCode"] == c, "PartCategory"
                        ].item(),
                    },
                    ignore_index=True,
                )
            else:
                for comp in comp_list["Component"].unique():
                    try:
                        items_p.loc[items_p["StockCode"] == comp, "QtyOnHand"] -= (
                            200
                            * comp_list.loc[
                                comp_list["Component"] == comp, "QtyPer"
                            ].item()
                        )

                    except Exception as e:
                        logger.error(e)

                kit_items = kit_items.append(
                    {
                        "StockCode": item,
                        "LimitComponent": c,
                        "Description": items_p.loc[
                            items_p["StockCode"] == c, "Description"
                        ].item(),
                        "CanMake": 200,
                        "MakeBuy": items_p.loc[
                            items_p["StockCode"] == c, "PartCategory"
                        ].item(),
                    },
                    ignore_index=True,
                )
        except Exception as e:

            logger.error("{} : {}".format(item, e))

    max_items = maxKit()

    with pd.ExcelWriter('DailyKittable.xlsx', engine='xlsxwriter') as writer:
        kit_items.to_excel(writer, sheet_name='kittable', index=False)
        max_items.to_excel(writer, sheet_name='max_kittable', index=False)

    logger.info("DailyKittable.xlsx file has been created.")

    try:
        daily_email("DailyKittable.xlsx")

    except Exception as e:

        logger.critical("FAILED TO SEND EMAIL : {}".format(e))


if __name__ == "__main__":
    try:
        logger.info("Running dailyKittable.py file")
        main()
        logger.info("Closing Connection to database")
        cnxn.close()
    except Exception as e:
        logging.critical("FAILLED TO EXECUTE : {}".format(e))
