import pandas as pd
import json
import os
from gretel_synthetics.config import LocalConfig
from gretel_synthetics.actgan import ACTGAN
from gretel_synthetics.generate import generate_actgan

# --- Configuration for Local gretel-synthetics ---
# Define output directory for model checkpoints and generated data
OUTPUT_DIR = "./gretel_synthetics_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Step 1: Define a Schema for SDTM DM Domain ---
# This schema drives the local ACTGAN model.
# The 'faker' providers are crucial here for generating plausible data
# since we are not training on an existing dataset.
sdtm_dm_schema_for_local = {
    "STUDYID": {"type": "string", "faker": "uuid4"},
    "DOMAIN": {"type": "string", "fixed_value": "DM"}, # 'fixed_value' ensures this is always 'DM'
    "USUBJID": {"type": "string", "faker": "uuid4"},
    "SUBJID": {"type": "string", "faker": "random_number(digits=5)"},
    "RFSTDTC": {"type": "string", "faker": "date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')"},
    "RFENDTC": {"type": "string", "faker": "date_between(start_date='-4y', end_date='today').strftime('%Y-%m-%d')"},
    "BRTHDTC": {"type": "string", "faker": "date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')"},
    "AGE": {"type": "int", "faker": "random_int(min=18, max=90)"},
    "SEX": {"type": "string", "faker": "random_element(elements=['F', 'M', 'U'])"},
    "RACE": {"type": "string", "faker": "random_element(elements=['WHITE', 'BLACK OR AFRICAN AMERICAN', 'ASIAN', 'AMERICAN INDIAN OR ALASKA NATIVE', 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER', 'MULTIPLE', 'OTHER', 'UNKNOWN'])"},
    "ETHNIC": {"type": "string", "faker": "random_element(elements=['HISPANIC OR LATINO', 'NOT HISPANIC OR LATINO', 'UNKNOWN'])"},
    "ARM": {"type": "string", "faker": "random_element(elements=['Placebo', 'Drug A Low Dose', 'Drug A High Dose'])"},
    "ARMCD": {"type": "string", "faker": "random_element(elements=['PLCBO', 'DRGALO', 'DRGAHI'])"} # Corrected: Missing closing double quote for faker string was here
}

# Convert schema to a list of dicts as expected by ACTGAN config
# And add necessary 'properties' for ACTGAN to correctly interpret types/faker.
# For schema-only generation, we create a dummy dataframe to pass to the model.
# The actual generation logic will rely on the `model_parameters.field_mapping`.
dummy_df = pd.DataFrame([
    {k: sdtm_dm_schema_for_local[k].get("faker", "str") for k in sdtm_dm_schema_for_local}
])

# Generate a list of field mapping dictionaries from the schema for ACTGAN
field_mapping = []
for field_name, field_props in sdtm_dm_schema_for_local.items():
    mapping_entry = {"name": field_name, "type": field_props["type"]}
    if "faker" in field_props:
        mapping_entry["faker"] = field_props["faker"]
    if "fixed_value" in field_props:
        mapping_entry["fixed_value"] = field_props["fixed_value"]
    field_mapping.append(mapping_entry)


# --- Step 2: Configure the Local Synthetic Model (ACTGAN) ---
# We use LocalConfig for local execution.
# For schema-only generation, we tell ACTGAN about the columns and faker mapping
# using model_parameters.
config = LocalConfig(
    input_data_path=None, # No input data for training, we're using schema-only generation
    output_dir=OUTPUT_DIR,
    # ACTGAN specific configurations
    actgan={
        "epochs": 100, # Number of training epochs (adjust for quality vs. time)
        "data_mode": "tabular",
        "verbose": False,
        "gen_lines": 100, # Number of synthetic records to generate
        "model_parameters": {
            "field_mapping": field_mapping,
            # If you had a real dataset, you'd also pass `nominal_columns` etc.
        }
    }
)

# --- Step 3: Train the Synthetic Model (Local Execution) ---
print("Initializing and training local ACTGAN model...")
try:
    # ACTGAN needs a dummy dataframe to initialize, even for schema-only generation
    # It uses this to infer column names and initial types if not fully specified by field_mapping
    model = ACTGAN(config)
    model.train_from_data(dummy_df) # Pass the dummy DataFrame
    print(f"Model training completed. Checkpoints saved to: {OUTPUT_DIR}/checkpoints")
except Exception as e:
    print(f"Error during model training: {e}")
    print("Ensure all necessary dependencies for gretel-synthetics (like SDV and Torch) are installed.")
    print("You might need to install them specifically: `pip install sdv torch`")
    exit()

# --- Step 4: Generate Synthetic Data ---
print("Generating synthetic SDTM DM data...")
try:
    # generate_actgan requires the trained model and the desired number of records
    synthetic_records = generate_actgan(model, num_records=100)
    print(f"Generated {len(synthetic_records)} records.")

    # Convert the list of dictionaries to a pandas DataFrame
    synthetic_df = pd.DataFrame(synthetic_records)

    # --- Step 5: Save and Display Generated Data ---
    synthetic_data_path = os.path.join(OUTPUT_DIR, "synthetic_sdtm_dm.csv")
    synthetic_df.to_csv(synthetic_data_path, index=False)
    print(f"Synthetic data saved to: {synthetic_data_path}")

    print("\n--- Sample of Generated SDTM DM Data ---")
    print(synthetic_df.head())
    print(f"\nTotal generated records: {len(synthetic_df)}")

    # Basic validation check (e.g., all DOMAIN values are 'DM')
    if 'DOMAIN' in synthetic_df.columns and all(synthetic_df['DOMAIN'] == 'DM'):
        print("Basic validation passed: All 'DOMAIN' values are 'DM'.")
    else:
        print("Basic validation failed: 'DOMAIN' column missing or values are not consistently 'DM'.")

except Exception as e:
    print(f"Error during data generation or saving: {e}")
