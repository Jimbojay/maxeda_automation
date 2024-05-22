import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
import re

load_dotenv()


gs1_file_path = os.getenv('path_datamodel_GS1')
datamodel_file_path = os.getenv('path_datamodel_maxeda')


###################
## GS1 datamodel
###################
print('### Read GS1 datamodel ###')
# Read the 'Bricks' to select only the attributes from active Bricks
gs1_df_bricks = pd.read_excel(gs1_file_path, sheet_name='Bricks', skiprows=3, dtype=str)
gs1_active_bricks_set = set(gs1_df_bricks[gs1_df_bricks['Brick activated'] == 'Yes']['Brick Code'].dropna())

# Read the Attributes per Brick sheet from the GS1 file to be able to address the attributes from active bricks
gs1_df_attributes_brick = pd.read_excel(gs1_file_path, sheet_name='Data for Attributes per Brick', skiprows=3, dtype=str)
# Filter gs1_df_attributes_brick for only those rows where the 'Brick' column's values are in gs1_active_bricks_set
gs1_df_attributes_brick_active = gs1_df_attributes_brick[gs1_df_attributes_brick['Brick'].isin(gs1_active_bricks_set)]

# Create a set of the 'FieldID' values from the filtered DataFrame
gs1_attributes_set = set(gs1_df_attributes_brick_active['FieldID'].dropna())


# Read metadata for attributes
gs1_df_attributes = pd.read_excel(gs1_file_path, sheet_name='Fielddefinitions', skiprows=3, dtype=str)


###################
## Maxeda datamodel
###################
print(f'### Read Maxeda datamodel ###')
# Read the 'S9 - Category' sheet from the Datamodel file
maxeda_df = pd.read_excel(datamodel_file_path, sheet_name='S7 - Attribute', dtype=str)

# Select relevant attributes
maxeda_df = maxeda_df[maxeda_df['Attribute Type'] == 'Category']

# Extract attribute code from Definition after "GS1 Field_ID "
# def extract_brick_code(definition):
#     if pd.isna(definition) or "GS1 Field_ID " not in definition:
#         return ''
#     start_index = definition.find("GS1 Field_ID ") + len("GS1 Field_ID ")
#     end_index = definition.find(' ', start_index)
#     if end_index == -1:  # No space found after the prefix
#         return definition[start_index:].strip()  # Return everything after the prefix, trimmed
#     return definition[start_index:end_index].strip()

# Extract attribute code from Definition between 2nd and 3rd space
# def extract_brick_code(definition):
#     # Convert to string in case the input is NaN or any other non-string data
#     definition = str(definition)pp
#     parts = definition.split()
#     if len(parts) > 3:
#         return parts[2].strip()
#     return ''

# Extract attribute code after "id" (case-insenstive)
def extract_brick_code(definition):
    # Convert to string in case the input is NaN or any other non-string data
    definition = str(definition)
    # Use a regular expression to find a pattern starting with 'id ' followed by non-space characters
    match = re.search(r"id (\S+)", definition, re.IGNORECASE)
    if match:
        return match.group(1).strip()  # Extract and return the part after 'id '
    return ''  # Return an empty string if no match is found

maxeda_df['Attribute code'] = maxeda_df['Definition'].apply(extract_brick_code)



# Exclude maxeda-attributes
maxeda_df = maxeda_df[~maxeda_df['Attribute code'].str.startswith("M")]

maxeda_attribute_set = set(maxeda_df['Attribute code'].replace('', np.nan).dropna())

####################
## Compare
####################

attribute_add_set = gs1_attributes_set - maxeda_attribute_set
attribute_delete_set = maxeda_attribute_set - gs1_attributes_set

# print(len(attribute_add_set))
print(len(attribute_delete_set))

# print(attribute_add_set)
print(attribute_delete_set)

####################
## Delete
####################

delete_attributes_df = maxeda_df[maxeda_df['Attribute code'].isin(attribute_delete_set)]
delete_attributes_df['Action']  = 'Delete'

print(delete_attributes_df)

####################
## Additions
####################

# Create an empty DataFrame with the same columns as maxeda_df
additions_attributes_df = pd.DataFrame(columns=maxeda_df.columns)

# Temporary list to hold all the new rows before concatenating them into the DataFrame
new_rows = []

# Iterate over the set to fill the DataFrame
for field_id in attribute_add_set:
    # Find the corresponding attribute name in gs1_df_attributes
    attribute_name = gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Attributename English'].iloc[0] if not gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Attributename English'].empty else ''
    
    # Create the new row data
    new_row = {
        'Definition': f"GS1 Field_ID {field_id} {attribute_name}",
        'Attribute Type': 'Category',
        'Attribute Parent Name': 'Category Specific Attributes',
        'Is Localizable': 'NO',
        'Is Complex': 'NO',
        'Is Required': 'NO',
        'Is ReadOnly': 'NO',
        'Is Hidden': 'NO',
        'Is Searchable': 'YES',
        'Is Null Value Search Required': 'YES',
        'Minimum Length': 0,
        'Maximum Length': 0,
        'Enable History': 'YES',
        'Apply Time Zone Conversion': 'NO'
    }
    
    # Add the new row to the list
    new_rows.append(new_row)

# Convert the list of dictionaries to DataFrame and concatenate it to the existing DataFrame
additions_attributes_df = pd.concat([additions_attributes_df, pd.DataFrame(new_rows)], ignore_index=True)

# Set default values for other columns not included in new_row if necessary
additions_attributes_df.fillna('', inplace=True)

print(additions_attributes_df)


###################
## Write output
###################

# Combine the two DataFrames into one new DataFrame
final_df = pd.concat([additions_attributes_df, delete_attributes_df], ignore_index=True)
final_df.drop(columns='Attribute code', inplace=True)

# Display the combined DataFrame
print(final_df)

print('### Output ###')

# Assuming 'filtered_maxeda_df' has been created as shown in the previous example
output_file_path = os.path.join(os.getcwd(), 'GS1_vs_Datamodel_Comparison_Attributes.xlsx')

# Use ExcelWriter to write DataFrame to an Excel file
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    final_df.to_excel(writer, sheet_name='S7 - Attribute', index=False)
            
# Load the updated Excel file into a DataFrame to confirm it saved correctly
loaded_attributes_df = pd.read_excel(output_file_path, sheet_name='S7 - Attribute')
print(len(loaded_attributes_df))