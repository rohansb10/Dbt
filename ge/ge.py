import great_expectations as gx
from pyspark.sql import SparkSession 
import yaml

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("spark_AdvancedChecks")
    .master("local[*]") 
    .getOrCreate()
)

# Sample data
data = [
    (1, "Alice", 23, "F", 50000),
    (2, "Bob", None, "M", 45000),
    (3, "Cathy", 35, "F", 62000),
    (4, "David", 17, "M", 29000),
    (5, "Eve", 29, "F", None),
]

columns = ["id", "name", "age", "gender", "salary"]
spark_df = spark.createDataFrame(data, columns)

# Get Great Expectations context
context = gx.get_context()

# Method 1: Load suite from YAML file
def load_suite_from_yaml(context, yaml_file_path):
    """Load expectation suite from YAML file"""
    with open(yaml_file_path, 'r') as file:
        suite_config = yaml.safe_load(file)
    
    # Create or update the suite
    try:
        suite = context.suites.add(gx.ExpectationSuite(**suite_config))
    except ValueError:
        # Suite might already exist, get it instead
        suite = context.suites.get(suite_config["name"])
    
    return suite

# Method 2: Create suite from YAML string (alternative approach)
def create_suite_from_yaml_string(context, yaml_string):
    """Create expectation suite from YAML string"""
    suite_config = yaml.safe_load(yaml_string)
    suite = context.suites.add(gx.ExpectationSuite(**suite_config))
    return suite

# Setup data source and batch
data_source = context.data_sources.add_spark(name="spark_inventory_parts")
data_asset = data_source.add_dataframe_asset(name="spark_inventory_parts_asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("spark_inventory_parts_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": spark_df})

# Load suite from YAML file
suite = load_suite_from_yaml(context, "spark_inventory_parts_suite.yml")

# Validate
validation_results = batch.validate(suite)

# Print results
print("\n===== Validation Summary =====")
print("Success:", validation_results.success)
for r in validation_results.results:
    print(f"{r.expectation_config.type} => {'PASSED' if r.success else 'FAILED'}")