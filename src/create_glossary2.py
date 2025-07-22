import pandas as pd
import json
import time
import re

# =====================================================================================
# --- Configuration ---
# YOU MUST UPDATE THESE VARIABLES TO MATCH YOUR ENVIRONMENT
# =====================================================================================

# 1. Define your main glossary's name and URN
GLOSSARY_NAME = "CDASPlatformGlossary"
GLOSSARY_URN = f"urn:li:glossaryNode:{GLOSSARY_NAME}"

# 2. Define how to build the URN for your datasets from the Excel tab name
#    Example for a Snowflake table: urn:li:dataset:(urn:li:dataPlatform:snowflake,your_db.your_schema.TABLE_NAME,PROD)
DATA_PLATFORM = "my-platform"  # e.g., "snowflake", "bigquery", "postgres"
URN_PATTERN = "my_database.my_schema.{table_name}"  # e.g., "prod_db.public.{table_name}"
ENVIRONMENT = "PROD"

# 3. Define the file paths and the user performing the ingestion
EXCEL_FILE_PATH = 'glossary_sample.xlsx'  # The name of your Excel file
OUTPUT_MCE_FILE = 'glossary_mce.json'
DATAHUB_ACTOR = 'urn:li:corpuser:datahub'  # The user URN


# =====================================================================================
# --- Helper Functions (No changes needed below this line) ---
# =====================================================================================

def generate_term_urn(term_name):
    """Generates a URN-friendly string from a term name."""
    clean_name = re.sub(r'[^a-zA-Z0-9_-]', '', term_name.replace(' ', ''))
    return f"urn:li:glossaryTerm:{clean_name}"


def create_main_glossary_node_mce():
    """Creates the MCE for the top-level glossary node."""
    return {
        "auditHeader": None,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.GlossaryNodeSnapshot": {
                "urn": GLOSSARY_URN,
                "aspects": [
                    {"com.linkedin.pegasus2avro.glossary.GlossaryNodeInfo": {"name": GLOSSARY_NAME}},
                    {"com.linkedin.pegasus2avro.common.Ownership": {
                        "owners": [{"owner": DATAHUB_ACTOR, "type": "DATAOWNER"}]}}
                ]
            }
        }
    }


def create_glossary_term_mce(row):
    """Creates an MCE for a child Glossary Term that belongs to the main node."""
    term_urn = generate_term_urn(row['Attribute Name'])
    return {
        "auditHeader": None,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.GlossaryTermSnapshot": {
                "urn": term_urn,
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.glossary.GlossaryTermInfo": {
                            "definition": str(row['Definition']),
                            "parentNode": GLOSSARY_URN,  # Links term to the main glossary node
                            "termSource": "CUSTOM_DATA_DICTIONARY"
                        }
                    }
                ]
            }
        }
    }


def create_editable_schema_metadata_mce(dataset_urn, field_docs):
    """Creates an MCE to document dataset columns."""
    return {
        "auditHeader": None,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                "urn": dataset_urn,
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.schema.EditableSchemaMetadata": {
                            "editableSchemaFieldInfo": field_docs,
                            "created": {"time": int(time.time() * 1000), "actor": DATAHUB_ACTOR},
                        }
                    }
                ]
            }
        }
    }


def main():
    """Main function to read all Excel sheets and generate MCEs."""
    all_mces = [create_main_glossary_node_mce()]
    created_term_urns = set()

    try:
        xls = pd.ExcelFile(EXCEL_FILE_PATH)
        for sheet_name in xls.sheet_names:
            print(f"Processing sheet: {sheet_name}...")
            df = pd.read_excel(xls, sheet_name=sheet_name)

            # Construct the Dataset URN for this sheet/table
            table_name_in_urn = URN_PATTERN.format(table_name=sheet_name)
            dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{DATA_PLATFORM},{table_name_in_urn},{ENVIRONMENT})"

            field_docs_for_dataset = []

            for _, row in df.iterrows():
                if not all(pd.notna(row.get(col)) for col in ['Attribute Name', 'Column Name', 'Definition']):
                    print(f"  Skipping row due to missing data: {row.to_dict()}")
                    continue

                # 1. Create the Glossary Term MCE
                term_urn = generate_term_urn(row['Attribute Name'])
                if term_urn not in created_term_urns:
                    all_mces.append(create_glossary_term_mce(row))
                    created_term_urns.add(term_urn)

                # 2. Prepare the documentation for this specific dataset field
                field_doc = {
                    "fieldPath": str(row['Column Name']),
                    "description": str(row['Definition']),
                    "glossaryTerms": {"terms": [{"urn": term_urn}]}
                }
                field_docs_for_dataset.append(field_doc)

            # 3. After processing all rows for the sheet, create the single dataset documentation MCE
            if field_docs_for_dataset:
                all_mces.append(create_editable_schema_metadata_mce(dataset_urn, field_docs_for_dataset))

        # Write all MCEs to a single file
        with open(OUTPUT_MCE_FILE, 'w') as f:
            json.dump(all_mces, f, indent=2)

        print(f"\nSuccessfully generated {len(all_mces)} MCEs in '{OUTPUT_MCE_FILE}'")

    except FileNotFoundError:
        print(f"Error: The file '{EXCEL_FILE_PATH}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    main()