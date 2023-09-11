'''import statements'''
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, col, row_number
from pyspark.sql.types import IntegerType
from src.util import util_functions

class VehicleAccidentsUS:
    '''class definition'''
    def __init__(self, path_to_config_file):
        input_file_paths = util_functions.load_config(path_to_config_file).get("INPUT_PATH")
        print('Reading yaml config file is successful')
        self.charges_df = util_functions.load_input(spark, input_file_paths.get("Charges"))
        self.endorsements_df = util_functions\
            .load_input(spark, input_file_paths.get("Endorsements"))
        self.restrict_df = util_functions.load_input(spark, input_file_paths.get("Restrict"))
        self.damages_df = util_functions.load_input(spark, input_file_paths.get("Damages"))
        self.primary_person_df = util_functions\
            .load_input(spark, input_file_paths.get("Primary_Person"))
        self.unit_df = util_functions.load_input(spark, input_file_paths.get("Unit"))
        print('Reading input csv files into spark dataframe is successful')

    def count_accidents_by_male(self, output_path, output_format):
        """
        Finds the crashes (accidents) in which number of persons killed are male
        """
        output_df = self.primary_person_df.filter((self.primary_person_df.DEATH_CNT == '1') \
                                         & (self.primary_person_df.PRSN_GNDR_ID == 'MALE'))

        util_functions.load_output(output_df, output_path, output_format)

        return output_df.select(countDistinct('CRASH_ID')).collect()[0][0]

    def count_2_wheeler_crashes(self, output_path, output_format):
        """
        Finds the crashes where the vehicle type was 2 wheeler.
        """
        output_df =  self.unit_df.where(self.unit_df.VEH_BODY_STYL_ID.contains('MOTORCYCLE'))

        util_functions.load_output(output_df, output_path, output_format)

        return output_df.select(countDistinct('CRASH_ID')).collect()[0][0]

    def get_state_with_highest_accident_by_female_drivers(self, output_path, output_format):
        """
        Finds state name with highest female accidents
        """
        output_df = self.primary_person_df.filter((self.primary_person_df\
                                                   .PRSN_GNDR_ID == "FEMALE") \
                                              & (~(self.primary_person_df.DRVR_LIC_STATE_ID\
                                                   .isin("NA","Other",'"Unknown"')))) \
                    .groupby("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc())

        util_functions.load_output(output_df, output_path, output_format)

        return output_df.select('DRVR_LIC_STATE_ID').first()[0]

    def get_top_vehicle_makers_with_highest_injuries(self, output_path, output_format):
        """
        Finds Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number
        of injuries including death
        """
        output_df = self.unit_df.filter(self.unit_df.VEH_MAKE_ID != "NA") \
            .withColumn('CASUALTIES_CNT', (col('TOT_INJRY_CNT').cast(IntegerType())) \
                        + (col('DEATH_CNT')).cast(IntegerType())) \
            .groupby("VEH_MAKE_ID").sum("CASUALTIES_CNT") \
            .withColumnRenamed("sum(CASUALTIES_CNT)", "TOTAL_CASUALTIES_CNT")\
            .withColumn('Top_Ranking', row_number()\
                        .over(Window.orderBy(col('TOTAL_CASUALTIES_CNT').desc()))) \
            .filter(col('Top_Ranking').between(5,15))

        util_functions.load_output(output_df, output_path, output_format)

        return [veh[0] for veh in output_df.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_user_group_for_body_styles(self, output_path, output_format):
        """
        Finds and show top ethnic user group of each unique body style that was involved in crashes
        """
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        output_df = self.unit_df.join(self.primary_person_df\
                                      , on=['CRASH_ID','UNIT_NBR'], how='left') \
            .filter(~self.unit_df.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                        "OTHER  (EXPLAIN IN NARRATIVE)"])) \
            .filter(~self.primary_person_df.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])) \
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count() \
            .withColumn("row", row_number().over(window_spec))\
                .filter(col("row") == 1).drop("row", "count")

        util_functions.load_output(output_df, output_path, output_format)

        return output_df

    def get_top_zip_codes_with_highest_crash(self, output_path, output_format):
        """
        Finds top 5 Zip Codes with the highest number crashes with alcohols
        as the contributing factor to a crash
        """
        output_df = self.primary_person_df.join(self.unit_df\
                                                , on=['CRASH_ID','UNIT_NBR'], how='left') \
            .dropna(subset=["DRVR_ZIP"]) \
            .filter(col('DRVR_ZIP').rlike('\d')) \
            .filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") \
                    | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL") \
                    | col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL")) \
            .groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)

        util_functions.load_output(output_df, output_path, output_format)

        return [row[0] for row in output_df.collect()]

    def get_crash_ids_with_no_damage_with_insurance(self, output_path, output_format):
        """
        Counts Distinct Crash IDs where No Damaged Property was observed and
        Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
        """
        output_df = self.unit_df.join(self.damages_df, on=["CRASH_ID"], how='left') \
            .filter((self.damages_df.DAMAGED_PROPERTY == "NONE") \
                    | (self.damages_df.DAMAGED_PROPERTY.isNull() == True)) \
            .filter(((self.unit_df.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & \
                     (~self.unit_df.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) \
                    | ((self.unit_df.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & \
                       (~self.unit_df.VEH_DMAG_SCL_2_ID\
                        .isin(["NA", "NO DAMAGE", "INVALID VALUE"])))) \
            .filter(self.unit_df.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")

        util_functions.load_output(output_df, output_path, output_format)

        return output_df.select(countDistinct('CRASH_ID')).collect()[0][0]

    def get_top_5_vehicle_makers(self, output_path, output_format):
        """
        Determines the Top 5 Vehicle Makes/Brands where drivers are charged
        with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours
        and has car licensed with the Top 25 states with highest number of offences
        """
        top_10_used_vehicle_colors = [row[0] for row in self.unit_df\
                                      .filter(self.unit_df.VEH_COLOR_ID != "NA")\
                    .groupby("VEH_COLOR_ID").count().orderBy(col("count").desc())\
                        .limit(10).collect()]
        top_25_highest_offences_state_list = [row[0] for row in self.unit_df\
                                              .filter(col("VEH_LIC_STATE_ID") != 'NA')\
                    .groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc())\
                        .limit(25).collect()]
        output_df = self.unit_df.join(self.primary_person_df, on=['CRASH_ID'], how='inner') \
            .join(self.charges_df, on=['CRASH_ID'], how='inner') \
            .filter(self.charges_df.CHARGE.contains("SPEED")) \
            .filter(self.primary_person_df.DRVR_LIC_TYPE_ID\
                    .isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])) \
            .filter(self.unit_df.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)) \
            .filter(self.unit_df.VEH_LIC_STATE_ID.isin(top_25_highest_offences_state_list)) \
            .groupby("VEH_MAKE_ID").count() \
            .orderBy(col("count").desc()).drop('count').limit(5)

        util_functions.load_output(output_df, output_path, output_format)

        return [row[0] for row in output_df.collect()]


if __name__ == '__main__':
    # Initializing spark session
    spark = SparkSession \
        .builder \
        .appName("Vehicle_Accidents_US_Analysis") \
        .getOrCreate()
    print('Spark Session is initialised : ', spark)

    CONFIG_FILE_PATH = "src/config/config.yaml"

    vehicle_accidents_config = VehicleAccidentsUS(CONFIG_FILE_PATH)
    output_file_paths = util_functions.load_config(CONFIG_FILE_PATH).get("OUTPUT_PATH")
    file_format = util_functions.load_config(CONFIG_FILE_PATH).get("FILE_FORMAT")

    # 1. Find the number of crashes (accidents) in which number of persons killed are male?
    print("Analytics 1 Result:", vehicle_accidents_config.\
          count_accidents_by_male(output_file_paths.get("Analytics_1"), file_format.get("Output")))

    # 2. How many two-wheelers are booked for crashes?
    print("Analytics 2 Result:", vehicle_accidents_config\
          .count_2_wheeler_crashes(output_file_paths.get("Analytics_2"), file_format.get("Output")))

    # 3. Which state has the highest number of accidents in which females are involved?
    print("Analytics 3 Result:", vehicle_accidents_config\
          .get_state_with_highest_accident_by_female_drivers(output_file_paths.get("Analytics_3")\
                                                             , file_format.get("Output")))

    # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a
    # largest number of injuries including death
    print("Analytics 4 Result:", vehicle_accidents_config\
          .get_top_vehicle_makers_with_highest_injuries(output_file_paths.get("Analytics_4")\
                                                        , file_format.get("Output")))

    # 5. For all the body styles involved in crashes,
    # mention the top ethnic user group of each unique body style
    print("Analytics 5 Result:")
    result_df = vehicle_accidents_config\
          .get_top_ethnic_user_group_for_body_styles(output_file_paths.get("Analytics_5")\
                                                     , file_format.get("Output"))
    result_df.show()

    # 6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes
    # with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    print("Analytics 6 Result:", vehicle_accidents_config\
          .get_top_zip_codes_with_highest_crash(output_file_paths.get("Analytics_6")\
                                                , file_format.get("Output")))

    # 7. Count of Distinct Crash IDs where No Damaged Property was observed and
    # Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    print("Analytics 7 Result:", vehicle_accidents_config\
          .get_crash_ids_with_no_damage_with_insurance(output_file_paths.get("Analytics_7")\
                                                       , file_format.get("Output")))

    # 8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged
    # with speeding related offences, has licensed Drivers,
    # uses top 10 used vehicle colours and has car licensed\ with the Top 25 states
    # with highest number of offences (to be deduced from the data)
    print("Analytics 8 Result:", vehicle_accidents_config\
          .get_top_5_vehicle_makers(output_file_paths.get("Analytics_8")\
                                    , file_format.get("Output")))

    spark.stop()
