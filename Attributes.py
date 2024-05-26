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

# Read picklists
gs1_df_picklists = pd.read_excel(gs1_file_path, sheet_name='Picklists', skiprows=3, dtype=str)

###################
## Maxeda datamodel
###################
print(f'### Read Maxeda datamodel ###')
# Read the 'S9 - Category' sheet from the Datamodel file
maxeda_s7_df = pd.read_excel(datamodel_file_path, sheet_name='S7 - Attribute')

# Select relevant attributes
maxeda_s7_df = maxeda_s7_df[maxeda_s7_df['Attribute Type'] == 'Category']

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

maxeda_s7_df['Attribute code'] = maxeda_s7_df['Definition'].apply(extract_brick_code)

#vanaf 15e karakter


# Exclude maxeda-attributes
maxeda_s7_df = maxeda_s7_df[~maxeda_s7_df['Attribute code'].str.startswith("M")]

maxeda_attribute_set = set(maxeda_s7_df['Attribute code'].replace('', np.nan).dropna())


# Read S8 - locale
# maxeda_s8_df = pd.read_excel(datamodel_file_path, sheet_name='S8 - Attribute - Locale')


####################
## Compare
####################

attribute_add_set = gs1_attributes_set - maxeda_attribute_set
attribute_delete_set = maxeda_attribute_set - gs1_attributes_set

# print(len(attribute_add_set))
# print(len(attribute_delete_set))

# print(attribute_add_set)
# print(attribute_delete_set)

####################
## Delete
####################

delete_attributes_df = maxeda_s7_df[maxeda_s7_df['Attribute code'].isin(attribute_delete_set)].copy()
delete_attributes_df['Action']  = 'Delete'

# Convert the 'Precision' column to string type
delete_attributes_df['Precision'] = delete_attributes_df['Precision'].astype(str)

delete_attributes_df.drop(columns='Attribute code', inplace=True)


# print(delete_attributes_df)

####################
## Additions
####################

# Get relevant rows from attribute overview
additions_attributes_df = gs1_df_attributes[gs1_df_attributes['FieldID'].isin(attribute_add_set)].copy()

# Data Type and display type
def determine_types(row):
    format = row['Format']
    decimals = row['Deci-\nmals']
    if format == "Number":
        data_type = "Integer" if decimals == '0' else "Decimal"
        display_type = "NumericTextBox"
    elif format == "DateTime":
        data_type = "DateTime"
        display_type = "DateTime"
    elif format == "Text":
        data_type = "String"
        display_type = "TextBox" 
    elif format == "Picklist (T/F)":
        data_type = "String"
        display_type = "LookupTable"
    elif format == "Picklist":
        data_type = "String"
        display_type = "LookupTable"
    elif format == "NumberPicklist":
        data_type = "Integer" if decimals == '0' else "Decimal"
        display_type = "NumericTextBox"
    elif format == "Boolean":
        data_type = "String"
        display_type = "LookupTable"
    else:
        data_type = "Unknown"
        display_type = "Unknown"
    return pd.Series([data_type, display_type], index=['INPUT_Data_type', 'INPUT_Display_type'])

# Apply the function to each row of the dataframe
additions_attributes_df[['INPUT_Data_type', 'INPUT_Display_type']] = additions_attributes_df.apply(determine_types, axis=1)

# 
additions_attributes_df['INPUT_Attribute_name'] = additions_attributes_df['Attributename English'].apply(
                                                lambda x: x[:x.rfind('(')].strip() if '(' in x and x.endswith(')') else x.strip()
                                            ).apply(lambda x: f"CatSpec_{x}")

# INPUT_Lookup_table_name
additions_attributes_df['INPUT_Lookup_table_name'] = np.select(
    [
        additions_attributes_df['Format'].isin(["Picklist (T/F)", "Boolean"]),
        additions_attributes_df['Format'] == "Picklist"
    ],
    [
        "YesNo",
        additions_attributes_df['INPUT_Attribute_name'].str.replace(r'\s+', '', regex=True).str.strip()
    ],
    default=""
)

# Add INPUT_Allowed_uoms
code_value_concat = gs1_df_picklists.groupby('Picklist ID')['Code value'].apply(lambda x: '||'.join(x.dropna().astype(str))).rename('INPUT_Allowed_uoms')

# Using a left join ensures all original rows in additions_attributes_df are retained
additions_attributes_df = additions_attributes_df.merge(code_value_concat, on='Picklist ID', how='left')

# Fill NaNs with empty strings if any picklist IDs didn't have code values
additions_attributes_df['INPUT_Allowed_uoms'] = additions_attributes_df['INPUT_Allowed_uoms'].fillna('')


#Fill the table
additions_attributes_df['ID'] = ''
additions_attributes_df['Action'] = ''
additions_attributes_df['Unique Identifier'] = ''
additions_attributes_df['Attribute Type'] = 'Category'
additions_attributes_df['Attribute Name'] = additions_attributes_df['INPUT_Attribute_name']
additions_attributes_df['Attribute Long Name'] = additions_attributes_df['Attributename English']
additions_attributes_df['Attribute Parent Name'] = 'Category Specific Attributes'
additions_attributes_df['Data Type'] = additions_attributes_df['INPUT_Data_type']
additions_attributes_df['Display Type'] = additions_attributes_df['INPUT_Display_type']
additions_attributes_df['Is Collection'] = np.where(
                                            additions_attributes_df['Repeat'].str.len() > 0, 'YES', 'NO'
                                        )
additions_attributes_df['Is Inheritable'] = 'NO' #For OneWS is 'YES' bij picklisten en numberpicklisten
additions_attributes_df['Is Localizable'] = 'NO'
additions_attributes_df['Is Complex'] = 'NO'
additions_attributes_df['Is Lookup'] =  np.where(additions_attributes_df['INPUT_Display_type'] == 'LookupTable', 'YES', 'NO')
additions_attributes_df['Is Required'] = 'NO'
additions_attributes_df['Is ReadOnly'] = 'NO'
additions_attributes_df['Is Hidden'] = 'NO'
additions_attributes_df['Show At Entity Creation?'] = 'YES'
additions_attributes_df['Is Searchable'] = 'YES'
additions_attributes_df['Is Null Value Search Required'] = 'YES'
additions_attributes_df['Generate Report Table Column?'] = ''
additions_attributes_df['Default Value'] = ''
additions_attributes_df['Minimum Length'] = 0  
additions_attributes_df['Maximum Length'] = 0  
additions_attributes_df['Range From'] = ''
additions_attributes_df['Is Range From Inclusive'] = ''
additions_attributes_df['Range To'] = ''
additions_attributes_df['Is Range To Inclusive'] = ''
additions_attributes_df['Precision'] = additions_attributes_df['Precision'] = additions_attributes_df['Deci-\nmals'].replace('0', '')
additions_attributes_df['Use Arbitrary Precision?'] = np.where(
                                                        additions_attributes_df['Precision'].str.len() > 0, 'NO', ''
                                                    )
additions_attributes_df['UOM Type'] = np.where(additions_attributes_df['Format'] == 'NumberPicklist', 'Custom UOM', '') #numberbicklist ? --> "CustomUOM",  bij onews "gdsn uom"
additions_attributes_df['Allowed UOMs'] = np.where(additions_attributes_df['Format'] == 'NumberPicklist', additions_attributes_df['INPUT_Allowed_uoms'],'') #ONLY FOR numberbicklist
additions_attributes_df['Default UOM'] = np.where(additions_attributes_df['Format'] == 'NumberPicklist', additions_attributes_df['UoM fixed'],'')
additions_attributes_df['Allowable Values'] = ''
additions_attributes_df['LookUp Table Name'] = additions_attributes_df['INPUT_Lookup_table_name']
additions_attributes_df['Lookup Display Columns'] = additions_attributes_df['LookUp Table Name']
additions_attributes_df['Lookup Search Columns'] = additions_attributes_df['LookUp Table Name']
additions_attributes_df['Lookup Display Format'] = additions_attributes_df['LookUp Table Name']
additions_attributes_df['Lookup Sort Order'] = additions_attributes_df['LookUp Table Name']
additions_attributes_df['Export Format'] = additions_attributes_df['LookUp Table Name']
additions_attributes_df['Sort Order'] = 0
additions_attributes_df['Definition'] = ("GS1 Field_ID " + 
                                         additions_attributes_df['FieldID'].astype(str) + " " + 
                                         additions_attributes_df['Definition English'])
additions_attributes_df['Example'] = ''
additions_attributes_df['Business Rule'] = ''
additions_attributes_df['Label'] = ''
additions_attributes_df['Extension'] = ''
additions_attributes_df['Web URI'] = ''
additions_attributes_df['Enable History'] = 'YES'
additions_attributes_df['Apply Time Zone Conversion'] = 'NO'
additions_attributes_df['Attribute Regular Expression'] = ''
additions_attributes_df['Is UOM Localizable'] = 'NO'

additions_attributes_df.fillna('', inplace=True)

# OneWs attributes
additions_attributes_onews_df = additions_attributes_df.copy()

# Setting 'Attribute Type' to 'Common' for all rows
additions_attributes_onews_df['Attribute Type'] = 'Common'

# Replacing 'CatSpec' with 'OneWS' in 'Attribute Name' and removing all spaces
additions_attributes_onews_df['Attribute Name'] = additions_attributes_onews_df['Attribute Name'].str.replace('CatSpec', 'OneWS').str.replace(' ', '')



# Adjust Is Inheritable for OneWs
additions_attributes_onews_df['Is Inheritable'] = np.where(
    (additions_attributes_df['Format'] == 'NumberPicklist') | (additions_attributes_df['Format'] == 'Picklist'),
    'YES',  # Set to 'YES' if 'Format' is either 'NumberPicklist' or 'Picklist'
    additions_attributes_onews_df['Is Inheritable']  # Keep the original value if conditions are False
)

# List of columns to set to empty strings
columns_to_clear = [
    'Precision', 'Use Arbitrary Precision?', 'UOM Type', 'Allowed UOMs', 'Default UOM',
    'LookUp Table Name', 'Lookup Display Columns', 'Lookup Search Columns', 'Lookup Display Format',
    'Lookup Sort Order', 'Export Format'
]

# Set the specified columns to empty strings
additions_attributes_onews_df[columns_to_clear] = ''


all_additions_attributes_df = pd.concat([additions_attributes_df, additions_attributes_onews_df], ignore_index=True)


#########
## Questions
#########
# OneWS attributes: Attribute Parent Name - seems to be mix
# OneWS attributes: Is Localizable - mix but default 'NO' for catspec
# OneWS attributes: Is complex - mix but default 'NO' for catspec
# OneWS attributes: Is read only - mix but default 'NO' for catspec
# OneWS attributes: Show at entity creation -  - mix but Kathy ruled default 'NO' for catspec
# OneWS attributes: Is searchable - mix but default 'YES' for catspec
# OneWS attributes: Is Null Value Search Required - mix but default 'YES' for catspec
# OneWS attributes: min and max length - these are defaut 0 with Catspec
# OneWS attributes: Apply Time Zone Conversion - mix but default 'NO' for catspec

#S8 - Attribute - Locale: deletions in S7 also trigger deletions in S8?

print(additions_attributes_onews_df['Is Inheritable'])

# Filter columns for the output
columns = [
    "ID", "Action", "Unique Identifier", "Attribute Type", "Attribute Name",
    "Attribute Long Name", "Attribute Parent Name", "Data Type", "Display Type",
    "Is Collection", "Is Inheritable", "Is Localizable", "Is Complex", "Is Lookup",
    "Is Required", "Is ReadOnly", "Is Hidden", "Show At Entity Creation?", "Is Searchable",
    "Is Null Value Search Required", "Generate Report Table Column?", "Default Value",
    "Minimum Length", "Maximum Length", "Range From", "Is Range From Inclusive",
    "Range To", "Is Range To Inclusive", "Precision", "Use Arbitrary Precision?",
    "UOM Type", "Allowed UOMs", "Default UOM", "Allowable Values", "LookUp Table Name",
    "Lookup Display Columns", "Lookup Search Columns", "Lookup Display Format",
    "Lookup Sort Order", "Export Format", "Sort Order", "Definition", "Example",
    "Business Rule", "Label", "Extension", "Web URI", "Enable History",
    "Apply Time Zone Conversion", "Attribute Regular Expression", "Is UOM Localizable"
]

all_additions_attributes_columns_df = all_additions_attributes_df.loc[:, columns]

# print(additions_attributes_df)
# exit()


# S8 - locales
additons_s8_df = all_additions_attributes_df.copy()

# Add Attribute Path
additons_s8_df['Attribute Path'] = additons_s8_df['Attribute Parent Name'] + '//' + additons_s8_df['Attribute Name']

###################
## Write output
###################

# Combine the two DataFrames into one new DataFrame
final_df = pd.concat([all_additions_attributes_columns_df, delete_attributes_df], ignore_index=True)

# Display the combined DataFrame
# print(final_df)

print('### Output ###')

output_file_path = os.path.join(os.getcwd(), 'GS1_vs_Datamodel_Comparison_Attributes.xlsx')

# Use ExcelWriter to write DataFrame to an Excel file
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    final_df.to_excel(writer, sheet_name='S7 - Attribute', index=False)
            
# Load the updated Excel file into a DataFrame to confirm it saved correctly
loaded_attributes_df = pd.read_excel(output_file_path, sheet_name='S7 - Attribute')
print(len(loaded_attributes_df))