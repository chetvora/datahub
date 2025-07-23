import pandas as pd
import json
import time
import re

# =====================================================================================
# --- Configuration ---
# YOU MUST UPDATE THESE VARIABLES TO MATCH YOUR ENVIRONMENT
# =====================================================================================

# 1. Define your main glossary's name
GLOSSARY_NAME = "MyPlatformGlossary"

# 2. Jira URL structure
JIRA_URL_PREFIX = "https://your-jira-instance.atlassian.net/browse/"

# 3. Configuration for DataStore1
DATASTORE1_PLATFORM = "snowflake"  # e.g., "snowflake", "bigquery", "postgres"
DATASTORE1_URN_PATTERN = "prod_db.public.{table_name}"

# 4. Configuration for DataStore2 (if you have one)
# If a row has a value in 'DataStore2 Column Name', it will use this config.
DATASTORE2_PLATFORM = "postgres"
DATASTORE2_URN_PATTERN = "analytics_db.reporting.{table_name}"

ENVIRONMENT = "PROD"
EXCEL_FILE_PATH = 'data_dictionary.xlsx'  # The name of your Excel file
OUTPUT_MCE_FILE = 'generated_mce.json'
DATAHUB_ACTOR = 'urn:li:corpuser:datahub'


# =====================================================================================
# --- Helper Functions (No changes needed below this line) ---
# =====================================================================================

def generate_urn(prefix, name):
    clean_name = re.sub(r'[^a-zA-Z0-9_-]', '', str(name).replace(' ', ''))
    return f"urn:li:{prefix}:{clean_name}"


def create_main_glossary_node_mce():
    return {"auditHeader": None, "proposedSnapshot": {
        "com.linkedin.pegasus2avro.metadata.snapshot.GlossaryNodeSnapshot": {
            "urn": generate_urn("glossaryNode", GLOSSARY_NAME),
            "aspects": [{"com.linkedin.pegasus2avro.glossary.GlossaryNodeInfo": {"name": GLOSSARY_NAME}}]}}}


def create_glossary_term_mce(row):
    term_urn = generate_urn("glossaryTerm", row['Attribute/Column Name'])

    # --- Build a rich definition ---
    definition_parts = []
    if pd.notna(row.get('Definition')):
        definition_parts.append(str(row['Definition']))
    if pd.notna(row.get('Syonym')):
        definition_parts.append(f"\n\n**Synonyms:** {row['Syonym']}")
    if pd.notna(row.get('List of Values')):
        definition_parts.append(f"\n\n**Accepted Values:**\n{row['List of Values']}")

    # --- Build Links ---
    links = []
    if pd.notna(row.get('Reference Link')):
        links.append({"url": str(row['Reference Link']), "description": "Reference Link"})
    if pd.notna(row.get('Jira Reference#')):
        jira_url = f"{JIRA_URL_PREFIX}{row['Jira Reference#']}"
        links.append({"url": jira_url, "description": "Jira Ticket"})

    # --- Build Tags ---
    tags = []
    if pd.notna(row.get('Originating System')):
        tags.append({"tag": generate_urn("tag", f"Source:{row['Originating System']}")})

    # --- Assemble Aspects ---
    aspects = [
        {"com.linkedin.pegasus2avro.glossary.GlossaryTermInfo": {
            "name": str(row['Full Name']),
            "definition": "\n".join(definition_parts),
            "parentNode": generate_urn("glossaryNode", GLOSSARY_NAME)
        }}
    ]
    if links:
        aspects.append({"com.linkedin.pegasus2avro.common.InstitutionalMemory": {"elements": links}})
    if tags:
        aspects.append({"com.linkedin.pegasus2avro.common.GlobalTags": {"tags": tags}})

    return {"auditHeader": None, "proposedSnapshot": {
        "com.linkedin.pegasus2avro.metadata.snapshot.GlossaryTermSnapshot": {"urn": term_urn, "aspects": aspects}}}


def create_editable_schema_metadata_mce(dataset_urn, field_docs):
    return {"auditHeader": None, "proposedSnapshot": {
        "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {"urn": dataset_urn, "aspects": [{
                                                                                                            "com.linkedin.pegasus2avro.schema.EditableSchemaMetadata": {
                                                                                                                "editableSchemaFieldInfo": field_docs,
                                                                                                                "created": {
                                                                                                                    "time": int(
                                                                                                                        time.time() * 1000),
                                                                                                                    "actor": DATAHUB_ACTOR}}}]}}}


def main():
    all_mces = [create_main_glossary_node_mce()]
    created_term_urns = set()

    # This will hold all column docs, grouped by the final dataset URN
    # e.g., {"urn:li:dataset...": [field_doc1, field_doc2]}
    dataset_docs = {}

    try:
        df = pd.read_excel(EXCEL_FILE_PATH)  # Assuming one sheet now
        for _, row in df.iterrows():
            if not pd.notna(row.get('Attribute/Column Name')):
                continue

            # 1. Create the Glossary Term MCE (if not already created)
            term_urn = generate_urn("glossaryTerm", row['Attribute/Column Name'])
            if term_urn not in created_term_urns:
                all_mces.append(create_glossary_term_mce(row))
                created_term_urns.add(term_urn)

            # --- Prepare documentation for both datastores ---
            table_name = row.get('physical dictionary table_name')
            if not pd.notna(table_name):
                continue

            # Handle DataStore 1
            ds1_col_name = row.get('DataStore1 Attribute/Column physical_name')
            if pd.notna(ds1_col_name):
                ds1_urn = f"urn:li:dataset:(urn:li:dataPlatform:{DATASTORE1_PLATFORM},{DATASTORE1_URN_PATTERN.format(table_name=table_name)},{ENVIRONMENT})"
                if ds1_urn not in dataset_docs:
                    dataset_docs[ds1_urn] = []
                dataset_docs[ds1_urn].append(
                    {"fieldPath": str(ds1_col_name), "glossaryTerms": {"terms": [{"urn": term_urn}]}})

            # Handle DataStore 2
            ds2_col_name = row.get('DataStore2 Column Name')
            if pd.notna(ds2_col_name):
                ds2_urn = f"urn:li:dataset:(urn:li:dataPlatform:{DATASTORE2_PLATFORM},{DATASTORE2_URN_PATTERN.format(table_name=table_name)},{ENVIRONMENT})"
                if ds2_urn not in dataset_docs:
                    dataset_docs[ds2_urn] = []
                dataset_docs[ds2_urn].append(
                    {"fieldPath": str(ds2_col_name), "glossaryTerms": {"terms": [{"urn": term_urn}]}})

        # 2. After processing all rows, create the MCEs for dataset documentation
        for dataset_urn, field_docs in dataset_docs.items():
            all_mces.append(create_editable_schema_metadata_mce(dataset_urn, field_docs))

        # Write all MCEs to the output file
        with open(OUTPUT_MCE_FILE, 'w') as f:
            json.dump(all_mces, f, indent=2)

        print(f"\nSuccessfully generated {len(all_mces)} MCEs in '{OUTPUT_MCE_FILE}'")

    except FileNotFoundError:
        print(f"Error: The file '{EXCEL_FILE_PATH}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    main()