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

# Get relevant rows from attribute overview
gs1_addition_attributes = gs1_df_attributes[gs1_df_attributes['FieldID'].isin(attribute_add_set)]

# Add data type
gs1_addition_attributes['INPUT_Data_type'] = gs1_addition_attributes.apply(
    lambda row: 'DateTime' if row['Format'] == 'DateTime'
                else 'String' if row['Format'] in ['Text', 'Picklist (T/F)', 'Picklist', 'Boolean']  # Added 'Boolean' here
                else 'Integer' if (row['Format'] == 'Number' or row['Format'] == 'NumberPicklist') and row['Deci-\nmals'] == '0'
                else 'Decimal' if row['Format'] == 'Number' or row['Format'] == 'NumberPicklist'
                else 'Unknown', axis=1)

# Create an empty DataFrame with the same columns as maxeda_df
additions_attributes_df = pd.DataFrame(columns=maxeda_df.columns)

# Temporary list to hold all the new rows before concatenating them into the DataFrame
new_rows = []

# Iterate over the set to fill the DataFrame
for field_id in attribute_add_set:
    # Find the corresponding attribute name in gs1_df_attributes
    attribute_name_long = gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Attributename English'].iloc[0] if not gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Attributename English'].empty else ''
    
    if attribute_name_long.endswith(')'):
        # Find the last occurrence of "(" and extract the string up to that point (excluding the last "(" and everything after)
        attribute_name = attribute_name_long[:attribute_name_long.rfind('(')].strip()
    else:
        attribute_name = attribute_name_long.strip()

    description = gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Definition English'].iloc[0] if not gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Definition English'].empty else ''
    format = gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Format'].iloc[0] if not gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Format'].empty else ''
    decimals = gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Deci-\nmals'].iloc[0] if not gs1_df_attributes.loc[gs1_df_attributes['FieldID'] == field_id, 'Deci-\nmals'].empty else ''

    # Determine data_type based on 'Format' and 'Decimals'
    if format == "Number":
        data_type = "Integer" if decimals == 0 else "Decimal"
        display_type = "NumericTextBox"
    elif format == "DateTime":
        data_type = "DateTime"
        display_type = "DateTime"
    elif format == "Text":
        data_type = "String"
        display_type = "t.b.d" # to discuss when text box, when text ares
    elif format == "Picklist (T/F)":
        data_type = "String"
        display_type = "LookupTable"
    elif format == "Picklist":
        data_type = "String"
        display_type = "LookupTable"
    elif format == "NumberPicklist":
        data_type = "Integer" if decimals == 0 else "Decimal"
        display_type = "NumericTextBox"
    elif format == "Boolean":
        data_type = "String"
        display_type = "LookupTable"
    else:
        data_type = "Unknown"
        display_type = "Unknown"

    if data_type == "Decimal":
        is_inheritable = "NO" # to discuss
        is_lookup = "NO"
        show_at_entity_creation = "YES" # to discuss
        precision = decimals
        use_arbitrary_precision = "NO"
        allowable_values= ''
        lookup_table_name= ''
        lookup_display_columns= ''	
        lookup_search_columns= ''
        lookup_display_format= ''
        lookup_sort_order= ''
        export_format= ''
    elif data_type == "Integer":
        is_inheritable = "NO" 
        is_lookup = "NO"
        show_at_entity_creation = "YES"
        precision = ''
        use_arbitrary_precision = ''
        allowable_values= ''
        lookup_table_name= ''
        lookup_display_columns= '' 	
        lookup_search_columns= ''
        lookup_display_format= ''
        lookup_sort_order= ''
        export_format= ''

    # Create the new row data
    new_row = {
        'Attribute Type': 'Category',
        'Attribute Name': f"CatSpec_{attribute_name}",
        'Attribute Long Name': attribute_name_long,
        'Attribute Parent Name': 'Category Specific Attributes',
        'Data Type' : data_type,
        'Display Type' : display_type,
        # 'Is Collection' : 't.b.d',
        'Is Inheritable' : is_inheritable,
        'Is Localizable': 'NO',
        'Is Complex': 'NO',
        'Is Lookup' : is_lookup,
        'Is Required': 'NO',
        'Is ReadOnly': 'NO',
        'Is Hidden': 'NO',
        'Show At Entity Creation?': show_at_entity_creation,
        'Is Searchable': 'YES',
        'Is Null Value Search Required': 'YES',
        'Minimum Length': 0,
        'Maximum Length': 0,
        'Precision' : precision,
        'Use Arbitrary Precision?' : use_arbitrary_precision,
        # 'UOM Type': 't.b.d.',
        'Allowed UOMs': 't.d.b.',	
        # 'Default UOM': 't.b.d.',
        'Allowable Values':	allowable_values,
        'LookUp Table Name': lookup_table_name,	
        'Lookup Display Columns': lookup_display_columns,	
        'Lookup Search Columns': lookup_search_columns,	
        'Lookup Display Format': lookup_display_format,	
        'Lookup Sort Order': lookup_sort_order,	
        'Export Format': export_format,
        # 'Sort Order' : 't.b.d',
        'Definition': f"GS1 Field_ID {field_id} {description}",
        'Enable History': 'YES',
        'Apply Time Zone Conversion': 'NO',
        'Is UOM Localizable': 'NO'
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