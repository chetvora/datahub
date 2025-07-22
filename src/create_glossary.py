import pandas as pd
import json
import time
import re

# --- Configuration ---
EXCEL_FILE_PATH = 'glossary_sample.xlsx'
OUTPUT_MCE_FILE = 'glossary_mce.json'
DATAHUB_ACTOR = 'urn:li:corpuser:datahub' # The user URN performing the ingestion

def generate_urn(term_name):
    """Generates a URN-friendly string from a term name."""
    # This creates a simple, clean URN. You can adjust the logic if needed.
    return re.sub(r'[^a-zA-Z0-9_-]', '', term_name.replace(' ', ''))

def create_glossary_term_mce(row, all_terms):
    """Creates a Metadata Change Event (MCE) for a single Glossary Term."""
    term_urn = f"urn:li:glossaryTerm:{generate_urn(row['TermName'])}"
    
    # Base structure for the GlossaryTermInfo aspect
    glossary_term_info = {
        "definition": str(row['Definition']),
        "termSource": str(row.get('TermSource', '')) # Safely get TermSource
    }

    # Add parent node if it exists in the Excel file
    if pd.notna(row.get('ParentTerm')):
        parent_name = row['ParentTerm']
        if parent_name in all_terms:
            glossary_term_info["parentNode"] = f"urn:li:glossaryTerm:{generate_urn(parent_name)}"

    # Full MCE snapshot structure
    mce = {
        "auditHeader": None,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.GlossaryTermSnapshot": {
                "urn": term_urn,
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.glossary.GlossaryTermInfo": glossary_term_info
                    },
                    {
                        "com.linkedin.pegasus2avro.common.Ownership": {
                            "owners": [{"owner": DATAHUB_ACTOR, "type": "DATAOWNER"}],
                            "lastModified": {"time": int(time.time() * 1000), "actor": DATAHUB_ACTOR}
                        }
                    }
                ]
            }
        },
        "proposedDelta": None
    }
    return mce

def main():
    """Main function to read Excel and generate MCEs."""
    all_mces = []
    
    try:
        glossary_df = pd.read_excel(EXCEL_FILE_PATH, sheet_name='GlossaryTerms')
        # Create a set of all term names for quick parent lookup
        all_term_names = set(glossary_df['TermName'])

        for _, row in glossary_df.iterrows():
            if pd.notna(row.get('TermName')):
                mce = create_glossary_term_mce(row, all_term_names)
                all_mces.append(mce)
            else:
                print(f"Skipping row due to missing 'TermName': {row}")

        # Write the list of MCEs to a single JSON file
        with open(OUTPUT_MCE_FILE, 'w') as f:
            json.dump(all_mces, f, indent=2)
        
        print(f"Successfully generated {len(all_mces)} glossary term MCEs in '{OUTPUT_MCE_FILE}'")

    except FileNotFoundError:
        print(f"Error: The file '{EXCEL_FILE_PATH}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
