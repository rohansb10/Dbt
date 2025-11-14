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

# Method 1: Load suite from YAML file (now used as primary)
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

# Method 2: Create suite from YAML string (alternative, not used here)
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

# Load suite from YAML file (ensure the file exists at the specified path)
yaml_path = r"C:\Users\rohan\Dbt\ge\spark_inventory_parts_suite.yml"
suite = load_suite_from_yaml(context, yaml_path)

# Validate
validation_results = batch.validate(suite)

# Improved Results Printing
def print_improved_validation_results(validation_results):
    """Print detailed and improved validation results"""
    print("\n" + "="*60)
    print("GREAT EXPECTATIONS VALIDATION REPORT")
    print("="*60)
    print(f"Overall Success: {'PASSED' if validation_results.success else 'FAILED'}")
    print(f"Total Expectations: {len(validation_results.results)}")
    
    passed_count = sum(1 for r in validation_results.results if r.success)
    failed_count = len(validation_results.results) - passed_count
    print(f"Passed: {passed_count} | Failed: {failed_count}")
    print("-"*60)
    
    for i, result in enumerate(validation_results.results, 1):
        status = "PASSED" if result.success else "FAILED"
        expectation_type = result.expectation_config.type
        kwargs = result.expectation_config.kwargs
        
        # Build a human-readable description
        description = f"{expectation_type} on column '{kwargs.get('column', 'table')}'"
        if 'type_' in kwargs:
            description += f" (expected type: {kwargs['type_']})"
        if 'min_value' in kwargs and 'max_value' in kwargs:
            description += f" (range: {kwargs['min_value']} to {kwargs['max_value']})"
        if 'value_set' in kwargs:
            description += f" (allowed values: {kwargs['value_set']})"
        if 'mostly' in kwargs:
            description += f" (mostly {kwargs['mostly']*100:.0f}%)"
        
        print(f"{i}. {description}")
        print(f"   Status: {status}")
        
        if not result.success:
            # Include failure details if available
            if hasattr(result, 'result') and 'unexpected_count' in result.result:
                unexpected_count = result.result['unexpected_count']
                print(f"   Details: {unexpected_count} unexpected values found.")
            if hasattr(result, 'exception_info') and result.exception_info:
                print(f"   Error: {result.exception_info['exception_message']}")
        print()

# Print improved results
print_improved_validation_results(validation_results)

# Stop Spark session
spark.stop()
