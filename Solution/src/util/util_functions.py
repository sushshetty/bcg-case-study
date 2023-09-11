''' import statement '''
import yaml


def load_config(file_path):
    """
    Loading config details into dictionary from given YAML format config file path
    input_params: file_path: file path to config.yaml
    return: dictionary with config details
    """
    with open(file_path, "r") as config_file:
        config_dict = yaml.safe_load(config_file)
        return config_dict



def load_input(spark, file_path):
    """
    Load CSV data into dataframe from given input path
    input_params: spark: spark instance, file_path: input path to the csv file
    return: dataframe
    """
    input_df = spark.read.option("inferSchema", "true").csv(file_path, header=True)
    return input_df



def load_output(output_df, file_path, write_format):
    """
    Load data from input dataframe to given output path as given input file format
    input_params: df: dataframe, file_path: output file path, write_format: Write file format
    """
    output_df.coalesce(1).write.format(write_format).mode('overwrite')\
        .option("header", "true").save(file_path)
        