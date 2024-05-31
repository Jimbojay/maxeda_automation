import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
import re
import openpyxl

load_dotenv()


gs1_file_path = os.getenv('path_datamodel_GS1')
datamodel_file_path = os.getenv('path_datamodel_maxeda')


###################
## GS1 datamodel
###################
print('### Read GS1 datamodel ###')

print('## Establish active brick ##')
# Read the 'Bricks' to select only the attributes from active Bricks
gs1_df_bricks = pd.read_excel(gs1_file_path, sheet_name='Bricks', skiprows=3, dtype=str)
gs1_active_bricks_set = set(gs1_df_bricks[gs1_df_bricks['Brick activated'] == 'Yes']['Brick Code'].dropna())

print('## Establish attributes from active brick ##')
# Read the Attributes per Brick sheet from the GS1 file to be able to address the attributes from active bricks
gs1_df_attributes_brick = pd.read_excel(gs1_file_path, sheet_name='Data for Attributes per Brick', skiprows=3, dtype=str)
# Filter gs1_df_attributes_brick for only those rows where the 'Brick' column's values are in gs1_active_bricks_set
gs1_df_attributes_brick_active = gs1_df_attributes_brick[gs1_df_attributes_brick['Brick'].isin(gs1_active_bricks_set)]

# Create a set of the 'FieldID' values from the filtered DataFrame
gs1_attributes_set = set(gs1_df_attributes_brick_active['FieldID'].dropna())

print('## Read attribute metadata ##')
# Read metadata for attributes
gs1_df_attributes = pd.read_excel(gs1_file_path, sheet_name='Fielddefinitions', skiprows=3, dtype=str)

print('## Read picklists ##')
# Read picklists
gs1_df_picklists = pd.read_excel(gs1_file_path, sheet_name='Picklists', skiprows=3, dtype=str)

###################
## Maxeda datamodel
###################
print(f'### Read Maxeda datamodel ###')
print(f'## Read S7 ##')
# Read the 'S7 - Attribute' sheet from the Datamodel file
maxeda_s7_df = pd.read_excel(datamodel_file_path, sheet_name='S7 - Attribute')

# Select relevant attributes
maxeda_s7_df = maxeda_s7_df[maxeda_s7_df['Attribute Type'] == 'Category']

# Extract attribute code from Definition after "GS1 Field_ID "
# def extract_attribute_code(definition):
#     if pd.isna(definition) or "GS1 Field_ID " not in definition:
#         return ''
#     start_index = definition.find("GS1 Field_ID ") + len("GS1 Field_ID ")
#     end_index = definition.find(' ', start_index)
#     if end_index == -1:  # No space found after the prefix
#         return definition[start_index:].strip()  # Return everything after the prefix, trimmed
#     return definition[start_index:end_index].strip()

# Extract attribute code from Definition between 2nd and 3rd space
# def extract_attribute_code(definition):
#     # Convert to string in case the input is NaN or any other non-string data
#     definition = str(definition)pp
#     parts = definition.split()
#     if len(parts) > 3:
#         return parts[2].strip()
#     return ''

#vanaf 15e karakter

# Extract attribute code after "id" (case-insenstive)
def extract_attribute_code(definition):
    # Convert to string in case the input is NaN or any other non-string data
    definition = str(definition)
    # Use a regular expression to find a pattern starting with 'id ' followed by non-space characters
    match = re.search(r"id (\S+)", definition, re.IGNORECASE)
    if match:
        return match.group(1).strip()  # Extract and return the part after 'id '
    return ''  # Return an empty string if no match is found

maxeda_s7_df['Attribute code'] = maxeda_s7_df['Definition'].apply(extract_attribute_code)

# Exclude maxeda-attributes
maxeda_s7_df = maxeda_s7_df[~maxeda_s7_df['Attribute code'].str.startswith("M")]

# Convert the 'Precision' column to string type
maxeda_s7_df['Precision'] = maxeda_s7_df['Precision'].astype(str)
# Use string manipulation to remove trailing '.0' from the string representation
maxeda_s7_df['Precision'] = maxeda_s7_df['Precision'].str.replace(r'\.0$', '', regex=True)
# Replace 'nan' strings with empty strings
maxeda_s7_df['Precision'] = maxeda_s7_df['Precision'].replace('nan', '')

maxeda_attribute_s7_set = set(maxeda_s7_df['Attribute code'].replace('', np.nan).dropna())


print(f'## Read S8 ##')
# S8 - Attribute - Locale
maxeda_s8_df = pd.read_excel(datamodel_file_path, sheet_name='S8 - Attribute - Locale')

# Select relevant attributes
maxeda_s8_df = maxeda_s8_df[maxeda_s8_df['Attribute Path'].str.startswith("Category Specific Attributes")]
# Extract attribute 
maxeda_s8_df['Attribute code'] = maxeda_s8_df['Definition'].apply(extract_attribute_code)
# Correct for the fact that not every attributeID is formatted with a dot on the second position
maxeda_s8_df['Attribute code'] = maxeda_s8_df['Attribute code'].apply(
    lambda x: x[0] + '.' + x[1:] if len(x) > 1 and x[1] != '.' else x
)

# Exclude maxeda-attributes
maxeda_s8_df = maxeda_s8_df[~maxeda_s8_df['Attribute code'].str.startswith("M")]


maxeda_attribute_s8_set = set(maxeda_s8_df['Attribute code'].replace('', np.nan).dropna())


# print(f'## Read S23 - Lookup Model ##')
# maxeda_s23_df = pd.read_excel(datamodel_file_path, sheet_name='S23 - Lookup Model')

# # a picklist value could have been added or deleted --> add/delete picklist value
# # a new picklist-attribute is introduced --> add picklist
# # a format change to picklist --> add picklist


# Directory containing the .xlsx files
directory_path = 'Workfiles/LookupData'

# List to store the transformed dataframes
combined_data = []


# ###############################
# # Load the Excel file
# file_path = 'Workfiles/LookupData/LookupData_112.xlsx'
# sheet_name = 'OneWSTypeOfDecoration'

# def read_excel_with_openpyxl(file_path, sheet_name):
#     workbook = openpyxl.load_workbook(file_path)
#     sheet = workbook[sheet_name]

#     data = []
#     for row in sheet.iter_rows(values_only=True):
#         data.append([str(cell).strip() if cell is not None else "" for cell in row])

#     df = pd.DataFrame(data[1:], columns=data[0])
#     return df

# df = read_excel_with_openpyxl(file_path, sheet_name)
# print(df)

# exit()
###############################

maxeda_lookuptables_s7_set = set(maxeda_s7_df['LookUp Table Name'].replace('', np.nan).dropna())

# Assuming gs1_df_picklists is already loaded correctly
valid_picklist_entries = set(zip(gs1_df_picklists['Picklist ID'], gs1_df_picklists['Code value']))

# def drop_duplicate_columns(df, file_name, sheet_name):
#     # Identify all duplicate columns (determine duplicates across all occurrences)
#     duplicates = df.columns[df.columns.duplicated()]
    
#     if not duplicates.empty:
#         print(f'Dropping duplicate columns in {file_name} - {sheet_name}: {duplicates.tolist()}')
#         # Drop duplicates, keep only the first occurrence
#         df = df.loc[:, ~df.columns.duplicated()]
    
#     return df



def read_excel_with_openpyxl(file_path, sheet_name):
    workbook = openpyxl.load_workbook(file_path, data_only=True)
    sheet = workbook[sheet_name]
    data = []

    for row in sheet.iter_rows(values_only=True):
        data.append([str(cell).strip() if cell is not None else "" for cell in row])

    # Check if the required headers are present and if the second column has at least one non-empty value
    if data and len(data[0]) >= 2 and data[0][0] == "Id" and "Code" in data[0][1]:
        df = pd.DataFrame(data[1:], columns=data[0])
        # Check if the second column contains at least one non-empty value
        if not df.iloc[:, 1].dropna().empty:

            # Rename the second column to "Code" explicitly
            # df.columns = [df.columns[0]] + ['Code'] + list(df.columns[2:])

            # # Prepare rename map for specific substrings in columns
            # rename_map = {}
            # for col in df.columns:
            #     if "en_US" in col:
            #         rename_map[col] = "en_EN"
            #     if "nl_NL" in col:
            #         rename_map[col] = "nl_NL"
            #     if "nl_BE" in col:
            #         rename_map[col] = "nl_BE"
            #     if "fr_FR" in col:
            #         rename_map[col] = "fr_FR"
            #     if "fr_BE" in col:
            #         rename_map[col] = "fr_BE"

            # df.rename(columns=rename_map, inplace=True)
            
            df['Sheet'] = sheet_name
            # df['FileName'] = os.path.basename(file_path)  # Store the file nam

            return df
    return None  # Return None if conditions are not met

def read_all_excel_files(directory_path):
    all_data_frames = []
    counter = 0
    for filename in os.listdir(directory_path):
        if filename.endswith('.xlsx'):
            counter += 1
            if counter< 1000:
                print(f"counter LookupData files: {counter}")
                file_path = os.path.join(directory_path, filename)
                workbook = openpyxl.load_workbook(file_path, data_only=True)

                # Iterate over each sheet except the first one
                for sheet_name in workbook.sheetnames[1:]:
                    if sheet_name in maxeda_lookuptables_s7_set:
                        # print(filename)
                        # print(sheet_name)
                        df = read_excel_with_openpyxl(file_path, sheet_name)
                        if df is not None:
                            # First, check the formatting of key columns just to be sure
                            df['Sheet'] = df['Sheet'].str.strip()
                            maxeda_s7_df['LookUp Table Name'] = maxeda_s7_df['LookUp Table Name'].str.strip()
                            # Perform the merge
                            df = df.merge(maxeda_s7_df[['LookUp Table Name', 'Attribute code']], left_on='Sheet', right_on='LookUp Table Name', how='left')

                            # Ensuring formatting is correct for the key columns
                            df['Attribute code'] = df['Attribute code'].str.strip()
                            gs1_df_attributes['FieldID'] = gs1_df_attributes['FieldID'].str.strip()
                            # Performing the merge to add 'Picklist ID'
                            df = df.merge(gs1_df_attributes[['FieldID', 'Picklist ID']], left_on='Attribute code', right_on='FieldID', how='left')

                            # Create a set of valid combinations from gs1_df_picklists
                            valid_combinations = set(zip(gs1_df_picklists['Picklist ID'], gs1_df_picklists['Code value']))

                            # Create a list to store the details of disregarded entries
                            disregarded_entries = []

                            # Define a function to filter rows and collect details of disregarded entries
                            def filter_valid_combinations(row, valid_combinations):
                                # Check if the combination of 'Picklist ID' and the value in the second column is in the valid combinations
                                if (row['Picklist ID'], row.iloc[1]) not in valid_combinations:
                                    # Collect filename, sheet name, and the second column's value for disregarded entries
                                    disregarded_entries.append({
                                        'filename': filename,  # Assumes 'filename' is defined where this function is called
                                        'sheet_name': sheet_name,  # Assumes 'sheet_name' is defined where this function is called
                                        'second_column_value': row.iloc[1]
                                    })
                                    return False
                                return True


                            # Apply the filtering function
                            filtered_df = df[df.apply(filter_valid_combinations, axis=1, args=(valid_combinations,))]

                            # columns_to_drop = ['Sheet', 'LookUp Table Name', 'Attribute code', 'FieldID', 'Picklist ID']
                            # filtered_df.drop(columns=columns_to_drop, errors='ignore')
                            
                            # Append only if there are disregarded entries
                            if disregarded_entries:
                                all_data_frames.append({'df': filtered_df, 'filename': filename, 'sheet_name': sheet_name})

                            # # Print details of the disregarded entries
                            # for entry in disregarded_entries:
                            #     print(f"Filename: {entry['filename']}, Sheet: {entry['sheet_name']}, Second Column Value: {entry['second_column_value']}")


    return all_data_frames          
        

directory_path = 'Workfiles/LookupData'
final_df = read_all_excel_files(directory_path)

        #             print(df)
        # if filename == 'LookupData_112.xlsx':
        #     exit()
        # if counter == 10:
        #     exit()

#     # Concatenate all data frames into one, if any valid sheets were processed
#     if all_data_frames:
#         final_df = pd.concat(all_data_frames, ignore_index=True)
#         return final_df
#     else:
#         return pd.DataFrame()  # Return an empty DataFrame if no sheets were added

# # Specify the directory path


# # exit()
# # # First, check the formatting of key columns just to be sure
# # final_df['Sheet'] = final_df['Sheet'].str.strip()
# # maxeda_s7_df['LookUp Table Name'] = maxeda_s7_df['LookUp Table Name'].str.strip()
# # # Perform the merge
# # final_df = final_df.merge(maxeda_s7_df[['LookUp Table Name', 'Attribute code']], left_on='Sheet', right_on='LookUp Table Name', how='left')

# # # Ensuring formatting is correct for the key columns
# # final_df['Attribute code'] = final_df['Attribute code'].str.strip()
# # gs1_df_attributes['FieldID'] = gs1_df_attributes['FieldID'].str.strip()
# # # Performing the merge to add 'Picklist ID'
# # final_df = final_df.merge(gs1_df_attributes[['FieldID', 'Picklist ID']], left_on='Attribute code', right_on='FieldID', how='left')


# # Directly converting to set, ensuring all values are unique
# # picklists_maxeda_set = set(final_df['Picklist ID'].dropna())

# print(final_df)

# # print("Sample 'Picklist ID' values from final_df:")
# # print(final_df['Picklist ID'].head())

# # print("Sample 'Picklist ID' values from gs1_df_picklists:")
# # print(gs1_df_picklists['Picklist ID'].head())


# # # exit()

# # Extract relevant columns and filter NaNs if necessary
# filtered_final_df = final_df[['Picklist ID', 'Code']].dropna(subset=['Picklist ID', 'Code'])

# # Create sets of code values for each DataFrame based on 'Picklist ID'
# codes_only_in_final_df = set(zip(filtered_final_df['Picklist ID'], filtered_final_df['Code'], filtered_final_df['FileName'], filtered_final_df['Sheet']))
# # codes_in_final_df = set(zip(filtered_final_df['Picklist ID'], filtered_final_df['Code']))

# # Extract just the 'Picklist IDs' from codes_in_final_df
# picklist_ids_in_final_df = {pid for pid, _ in codes_only_in_final_df}

# # Filter gs1_df_picklists to include only relevant 'Picklist IDs'
# filtered_gs1_picklists = gs1_df_picklists[gs1_df_picklists['Picklist ID'].isin(picklist_ids_in_final_df)]

# # Create the set of (Picklist ID, Code value) tuples with the filtered DataFrame
# codes_only_in_gs1_picklists = set(zip(filtered_gs1_picklists['Picklist ID'], filtered_gs1_picklists['Code value'], ['']*len(filtered_gs1_picklists), ['']*len(filtered_gs1_picklists)))
# # codes_in_gs1_picklists = set(zip(filtered_gs1_picklists['Picklist ID'], filtered_gs1_picklists['Code value']))

# # Find codes in final_df not in gs1_df_picklists
# codes_only_in_final_df = codes_only_in_final_df - codes_only_in_gs1_picklists

# # Find codes in gs1_df_picklists not in final_df
# codes_only_in_gs1_picklists = codes_only_in_gs1_picklists - codes_only_in_final_df

# # Print discrepancies
# print("Codes in final_df but not in gs1_df_picklists:", codes_only_in_final_df)
# print("Codes in gs1_df_picklists but not in final_df:", codes_only_in_gs1_picklists)

# # exit()


####################
## Pre-calculations for possible additions
####################
print(f'### Pre-calculations for possible additions ###')

# combined_attribute_set = attribute_add_s7_set.union(attribute_overlap_s7_set)

# Get relevant rows from attribute overview
gs1_df_attributes_processed = gs1_df_attributes.copy()


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
gs1_df_attributes_processed[['INPUT_Data_type', 'INPUT_Display_type']] = gs1_df_attributes_processed.apply(determine_types, axis=1)

# 
gs1_df_attributes_processed['INPUT_Attribute_name'] = gs1_df_attributes_processed['Attributename English'].apply(
                                                lambda x: x[:x.rfind('(')].strip() if '(' in x and x.endswith(')') else x.strip()
                                            ).apply(lambda x: f"CatSpec_{x}")

# INPUT_Lookup_table_name
gs1_df_attributes_processed['INPUT_Lookup_table_name'] = np.select(
    [
        gs1_df_attributes_processed['Format'].isin(["Picklist (T/F)", "Boolean"]),
        gs1_df_attributes_processed['Format'] == "Picklist"
    ],
    [
        "YesNo",
        gs1_df_attributes_processed['INPUT_Attribute_name'].str.replace(r'\s+', '', regex=True).str.strip()
    ],
    default=""
)

# Add INPUT_Allowed_uoms
code_value_concat = gs1_df_picklists.groupby('Picklist ID')['Code value'].apply(lambda x: '||'.join(x.dropna().astype(str))).rename('INPUT_Allowed_uoms')

# Using a left join ensures all original rows in gs1_df_attributes_processed are retained
gs1_df_attributes_processed = gs1_df_attributes_processed.merge(code_value_concat, on='Picklist ID', how='left')

# Fill NaNs with empty strings if any picklist IDs didn't have code values
gs1_df_attributes_processed['INPUT_Allowed_uoms'] = gs1_df_attributes_processed['INPUT_Allowed_uoms'].fillna('')


#Fill the table
gs1_df_attributes_processed['ID'] = ''
gs1_df_attributes_processed['Action'] = ''
gs1_df_attributes_processed['Unique Identifier'] = ''
gs1_df_attributes_processed['Attribute Type'] = 'Category'
gs1_df_attributes_processed['Attribute Name'] = gs1_df_attributes_processed['INPUT_Attribute_name']
gs1_df_attributes_processed['Attribute Long Name'] = gs1_df_attributes_processed['Attributename English']
gs1_df_attributes_processed['Attribute Parent Name'] = 'Category Specific Attributes'
gs1_df_attributes_processed['Data Type'] = gs1_df_attributes_processed['INPUT_Data_type']
gs1_df_attributes_processed['Display Type'] = gs1_df_attributes_processed['INPUT_Display_type']
gs1_df_attributes_processed['Is Collection'] = np.where(
                                            gs1_df_attributes_processed['Repeat'].str.len() > 0, 'YES', 'NO'
                                        )
gs1_df_attributes_processed['Is Inheritable'] = 'NO' #For OneWS is 'YES' bij picklisten en numberpicklisten
gs1_df_attributes_processed['Is Localizable'] = 'NO'
gs1_df_attributes_processed['Is Complex'] = 'NO'
gs1_df_attributes_processed['Is Lookup'] =  np.where(gs1_df_attributes_processed['INPUT_Display_type'] == 'LookupTable', 'YES', 'NO')
gs1_df_attributes_processed['Is Required'] = 'NO'
gs1_df_attributes_processed['Is ReadOnly'] = 'NO'
gs1_df_attributes_processed['Is Hidden'] = 'NO'
gs1_df_attributes_processed['Show At Entity Creation?'] = 'YES'
gs1_df_attributes_processed['Is Searchable'] = 'YES'
gs1_df_attributes_processed['Is Null Value Search Required'] = 'YES'
gs1_df_attributes_processed['Generate Report Table Column?'] = ''
gs1_df_attributes_processed['Default Value'] = ''
gs1_df_attributes_processed['Minimum Length'] = 0  
gs1_df_attributes_processed['Maximum Length'] = 0  
gs1_df_attributes_processed['Range From'] = ''
gs1_df_attributes_processed['Is Range From Inclusive'] = ''
gs1_df_attributes_processed['Range To'] = ''
gs1_df_attributes_processed['Is Range To Inclusive'] = ''
gs1_df_attributes_processed['Precision'] = gs1_df_attributes_processed['Precision'] = gs1_df_attributes_processed['Deci-\nmals'].replace('0', '')
gs1_df_attributes_processed['Use Arbitrary Precision?'] = np.where(
                                                        gs1_df_attributes_processed['Precision'].str.len() > 0, 'NO', ''
                                                    )
gs1_df_attributes_processed['UOM Type'] = np.where(gs1_df_attributes_processed['Format'] == 'NumberPicklist', 'Custom UOM', '') #numberbicklist ? --> "CustomUOM",  bij onews "gdsn uom"
gs1_df_attributes_processed['Allowed UOMs'] = np.where(gs1_df_attributes_processed['Format'] == 'NumberPicklist', gs1_df_attributes_processed['INPUT_Allowed_uoms'],'') #ONLY FOR numberbicklist
gs1_df_attributes_processed['Default UOM'] = np.where(gs1_df_attributes_processed['Format'] == 'NumberPicklist', gs1_df_attributes_processed['UoM fixed'],'')
gs1_df_attributes_processed['Allowable Values'] = ''
gs1_df_attributes_processed['LookUp Table Name'] = gs1_df_attributes_processed['INPUT_Lookup_table_name']
gs1_df_attributes_processed['Lookup Display Columns'] = gs1_df_attributes_processed['LookUp Table Name']
gs1_df_attributes_processed['Lookup Search Columns'] = gs1_df_attributes_processed['LookUp Table Name']
gs1_df_attributes_processed['Lookup Display Format'] = gs1_df_attributes_processed['LookUp Table Name']
gs1_df_attributes_processed['Lookup Sort Order'] = gs1_df_attributes_processed['LookUp Table Name']
gs1_df_attributes_processed['Export Format'] = gs1_df_attributes_processed['LookUp Table Name']
gs1_df_attributes_processed['Sort Order'] = 0
gs1_df_attributes_processed['Definition'] = ("GS1 Field_ID " + 
                                         gs1_df_attributes_processed['FieldID'].astype(str) + " " + 
                                         gs1_df_attributes_processed['Definition English'])
gs1_df_attributes_processed['Example'] = ''
gs1_df_attributes_processed['Business Rule'] = ''
gs1_df_attributes_processed['Label'] = ''
gs1_df_attributes_processed['Extension'] = ''
gs1_df_attributes_processed['Web URI'] = ''
gs1_df_attributes_processed['Enable History'] = 'YES'
gs1_df_attributes_processed['Apply Time Zone Conversion'] = 'NO'
gs1_df_attributes_processed['Attribute Regular Expression'] = ''
gs1_df_attributes_processed['Is UOM Localizable'] = 'NO'

gs1_df_attributes_processed.fillna('', inplace=True)

####################
## S7 
####################
print(f'### S7 ###')

    ####################
    ## Establish set of attributes for 1) additions, 2) deletions, and 3) overlapping 
    ####################

attribute_add_s7_set = gs1_attributes_set - maxeda_attribute_s7_set
attribute_delete_s7_set = maxeda_attribute_s7_set - gs1_attributes_set
attribute_overlap_s7_set = gs1_attributes_set & maxeda_attribute_s7_set

# print(len(attribute_add_s7_set))
# print(len(attribute_delete_s7_set))

# print(attribute_add_s7_set)
# print(attribute_delete_s7_set)

    ####################
    ## Delete
    ####################
print(f'## Delete ##')

def delete_attributes(delete_set, df, return_df):

    temp_df = df[df['Attribute code'].isin(delete_set)].copy()
    temp_df['Action'] = 'Delete'
    return_df = pd.concat([return_df, temp_df], ignore_index=True)
    return return_df

# Create an empty delete_attributes_s7_df with necessary columns
delete_attributes_s7_df = pd.DataFrame(columns=list(maxeda_s7_df.columns))

delete_attributes_s7_df = delete_attributes(attribute_delete_s7_set, maxeda_s7_df, delete_attributes_s7_df)
# print(delete_attributes_s7_df)

    ####################
    ## Additions
    ####################
print(f'## Add ##')
def add_attributes_s7(add_set, all_additions_attributes_s7_df):

    additions_attributes_s7_df = gs1_df_attributes_processed[gs1_df_attributes_processed['FieldID'].isin(add_set)].copy()

    # OneWs attributes
    additions_attributes_onews_s7_df = additions_attributes_s7_df.copy()

    # Setting 'Attribute Type' to 'Common' for all rows
    additions_attributes_onews_s7_df['Attribute Type'] = 'Common'

    # Replacing 'CatSpec' with 'OneWS' in 'Attribute Name' and removing all spaces
    additions_attributes_onews_s7_df['Attribute Name'] = additions_attributes_onews_s7_df['Attribute Name'].str.replace('CatSpec', 'OneWS').str.replace(' ', '')

    # Adjust Is Inheritable for OneWs
    additions_attributes_onews_s7_df['Is Inheritable'] = np.where(
        (additions_attributes_s7_df['Format'] == 'NumberPicklist') | (additions_attributes_s7_df['Format'] == 'Picklist'),
        'YES',  # Set to 'YES' if 'Format' is either 'NumberPicklist' or 'Picklist'
        additions_attributes_onews_s7_df['Is Inheritable']  # Keep the original value if conditions are False
    )

    # Update 'Data Type' and 'Display Type' based on 'Format'
    additions_attributes_s7_df['Data Type'] = np.where(
        additions_attributes_s7_df['Format'] == 'Boolean',
        'Boolean',  # Set 'Data Type' to 'Boolean' if 'Format' is 'Boolean'
        additions_attributes_s7_df['Data Type']  # Otherwise, keep the current value
    )

    additions_attributes_s7_df['Display Type'] = np.where(
        additions_attributes_s7_df['Format'] == 'Boolean',
        'DropDown',  # Set 'Display Type' to 'DropDown' if 'Format' is 'Boolean'
        additions_attributes_s7_df['Display Type']  # Otherwise, keep the current value
    )

    # Adjust Is Inheritable 
    additions_attributes_onews_s7_df['Is Inheritable'] = np.where(
        (additions_attributes_s7_df['Format'] == 'NumberPicklist') | (additions_attributes_s7_df['Format'] == 'Picklist'),
        'YES',  # Set to 'YES' if 'Format' is either 'NumberPicklist' or 'Picklist'
        additions_attributes_onews_s7_df['Is Inheritable']  # Keep the original value if conditions are False
    )

    # List of columns to set to empty strings
    columns_to_clear_s7 = [
        'Precision', 'Use Arbitrary Precision?', 'UOM Type', 'Allowed UOMs', 'Default UOM',
        'LookUp Table Name', 'Lookup Display Columns', 'Lookup Search Columns', 'Lookup Display Format',
        'Lookup Sort Order', 'Export Format'
    ]

    # Set the specified columns to empty strings
    additions_attributes_onews_s7_df[columns_to_clear_s7] = ''


    all_additions_attributes_s7_df = pd.concat([all_additions_attributes_s7_df, additions_attributes_s7_df, additions_attributes_onews_s7_df], ignore_index=True)
    return all_additions_attributes_s7_df


# Create an empty delete_attributes_s7_df with necessary columns
all_additions_attributes_s7_df = pd.DataFrame(columns=list(gs1_df_attributes_processed.columns))

all_additions_attributes_s7_df = add_attributes_s7(attribute_add_s7_set, all_additions_attributes_s7_df)

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

# Changes: Allowed and Default UOM vaak andere notaties van hetzelfde. Deze meenemen? Net zoals precision of TextArea → texbox

# S8 - Attribute - Locale: deletions in S7 also trigger deletions in S8?

# No attribute code consitently for every attribute in S8, a lot are also missing for Category Specific attributes, crucial for additions and deletions (7K/16K)

# LookupTables only include sheets where code column does not include "*" in the header?

    ####################
    ## Changes
    ####################
print(f'## Changes ##')
overlap_attributes_df = gs1_df_attributes_processed[gs1_df_attributes_processed['FieldID'].isin(attribute_overlap_s7_set)].copy()

# Step 1: Duplicate and prefix selected columns in maxeda_s7_df to compare including the key being attribute code
columns_to_copy = ['Attribute code','Data Type', 'Display Type', 'Precision', 'Allowed UOMs', 'Default UOM', 'Is Collection']

# Create a new DataFrame, maxeda_s7_changes_df, with only the specified columns from maxeda_s7_df
maxeda_s7_changes_df = maxeda_s7_df[columns_to_copy].copy()

# Remove 'Attribute code' from the list as we want to loop over the rest to compare
columns_to_copy.remove('Attribute code')

# Prefix check-columns
rename_dict = {col: 'ORIGINAL_' + col for col in columns_to_copy}

# Rename the columns using the dictionary
maxeda_s7_changes_df.rename(columns=rename_dict, inplace=True)

# Step 2: Perform a left join from overlap_attributes_df to the modified maxeda_s7_df
merged_df = overlap_attributes_df.merge(
    maxeda_s7_changes_df,
    left_on='FieldID',
    right_on='Attribute code',
    how='left'
)

# Step 3: Compare original and newly added values, collect discrepancies and reasons
discrepancy_details = []
for col in columns_to_copy:
    for index, row in merged_df.iterrows():
        # Trim values and check if they are not empty
        original_trimmed = (str(row['ORIGINAL_' + col]).strip() if pd.notnull(row['ORIGINAL_' + col]) else '')
        new_trimmed = (str(row[col]).strip() if pd.notnull(row[col]) else '')
        
        # Add to discrepancy details only if both are not empty and different
        if (original_trimmed or new_trimmed) and (original_trimmed != new_trimmed):
            discrepancy_details.append({
                'FieldID': row['FieldID'],
                'Column': col,
                'Original Value': row['ORIGINAL_' + col],
                'New Value': row[col]
            })


# Convert discrepancy details to a DataFrame for better visualization and analysis
discrepancy_df = pd.DataFrame(discrepancy_details)


# Step 4: Extract unique FieldIDs with discrepancies
# unique_discrepancy_field_ids = discrepancy_df['FieldID'].unique()
change_set = set(discrepancy_df['FieldID'].dropna())

all_additions_attributes_s7_df = add_attributes_s7(change_set, all_additions_attributes_s7_df)

delete_attributes_s7_df = delete_attributes(change_set, maxeda_s7_df, delete_attributes_s7_df)

# Display the unique FieldIDs and discrepancy details
# print("Unique FieldIDs with discrepancies:", change_set)
# print("\nDiscrepancy Details:")
# print(discrepancy_df.columns)

# print(additions_attributes_s7_df)
# exit()


####################
## S8 
####################

print(f'### S8  ###')

    ####################
    ## Establish set of attributes for 1) additions, 2) deletions, and 3) overlapping 
    ####################

attribute_add_s8_set = gs1_attributes_set - maxeda_attribute_s8_set
attribute_delete_s8_set = maxeda_attribute_s8_set - gs1_attributes_set

    ####################
    ## Delete
    ####################

# Create an empty delete_attributes_s8_df with necessary columns
delete_attributes_s8_df = pd.DataFrame(columns=list(maxeda_s8_df.columns))

delete_attributes_s8_df = delete_attributes(attribute_delete_s8_set, maxeda_s8_df,  delete_attributes_s8_df)

    ####################
    ## Additions
    ####################
print(f'## Add ##')

# Initiat the dataset
additions_attributes_s8_df = gs1_df_attributes_processed[gs1_df_attributes_processed['FieldID'].isin(attribute_add_s8_set)].copy()

# Contruct 'Attribute Path'
additions_attributes_s8_df['Attribute Path'] = additions_attributes_s8_df['Attribute Parent Name'] + '//' + additions_attributes_s8_df['Attribute Name']

# print(additions_attributes_s8_df.columns)
# exit()

# Create an empty delete_attributes_s7_df with necessary columns
additons_s8_locale_df = pd.DataFrame(columns=list(additions_attributes_s8_df.columns))

# Create a DataFrame with repeated rows, each original row appearing four times for different locales
locales = ['nl_BE', 'nl_NL', 'fr_BE', 'fr_FR']
additons_s8_locale_df = pd.DataFrame(
    np.repeat(additions_attributes_s8_df.values, len(locales), axis=0),  # Repeat each row
    columns=additions_attributes_s8_df.columns
)

additons_s8_locale_df['Locale'] = np.tile(locales, len(additions_attributes_s8_df))  # Assign locales repeatedly for each group of rows

# Define 'Attribute Long Name' based on 'Locale'
additons_s8_locale_df['Attribute Long Name'] = np.where(
    additons_s8_locale_df['Locale'].isin(['nl_BE', 'nl_NL']),
    additions_attributes_s8_df['Attributename Dutch'].repeat(len(locales)).values,  # Repeat values to match the new row expansion
    additions_attributes_s8_df['Attributename French'].repeat(len(locales)).values
)

#Rename headers
additons_s8_locale_df.rename(columns={'Minimum Length': 'Min Length', 'Maximum Length': 'Max Length'}, inplace=True)


# Empty all values in 'Min Length' and 'Max Length'
additons_s8_locale_df['Min Length'] = ''
additons_s8_locale_df['Max Length'] = ''

# Onews
additions_onews_s8_locale_df = additons_s8_locale_df.copy()
# Replacing 'CatSpec' with 'OneWS' in 'Attribute Name' and removing all spaces
additions_onews_s8_locale_df['Attribute Name'] = additions_onews_s8_locale_df['Attribute Name'].str.replace('CatSpec', 'OneWS').str.replace(' ', '').str.replace('Category Specific Attributes//', 'OneWS_XXXX//')


all_additions_attributes_s8_df = pd.concat([additons_s8_locale_df, additions_onews_s8_locale_df], ignore_index=True)

####################
## S8 
####################

print(f'### S8  ###')
all_additions_attributes_s8_df


###################
## Write output
###################
print('### Output ###')
print('## S7 ##')
# Combine the two DataFrames into one new DataFrame
final_s7_df = pd.concat([delete_attributes_s7_df, all_additions_attributes_s7_df], ignore_index=True)

# Filter columns for the output
columns_s7 = [
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

final_s7_df = final_s7_df.loc[:, columns_s7]

print('## S8 ##')
# Filtering the columns for the output as per specification

final_s8_df = pd.concat([delete_attributes_s8_df, all_additions_attributes_s8_df], ignore_index=True)


columns_s8 = ['ID', 'Action', 'Unique Identifier', 'Attribute Path', 'Locale', 'Attribute Long Name', 'Min Length', 'Max Length', 'Definition', 'Example', 'Business Rule']
final_s8_df = final_s8_df[columns_s8]


output_file_path_attributes = os.path.join(os.getcwd(), 'GS1_vs_Datamodel_Comparison_Attributes.xlsx')
output_file_path_lookupdata = os.path.join(os.getcwd(), 'LookupData.xlsx')

print('## Output writer ##')
# Use ExcelWriter to write DataFrame to an Excel file
with pd.ExcelWriter(output_file_path_attributes, engine='openpyxl') as writer:
    final_s7_df.to_excel(writer, sheet_name='S7 - Attribute', index=False)
    final_s8_df.to_excel(writer, sheet_name='S8 - Attribute - Locale', index=False)
    
# Use ExcelWriter to write DataFrame to an Excel file
with pd.ExcelWriter(output_file_path_lookupdata, engine='openpyxl') as writer:
    # Create a DataFrame to store summary information about the sheets
    summary_data = {
        'LookupTableName': [item['sheet_name'] for item in final_df],
        'SheetName': [item['sheet_name'] for item in final_df],
        'Load Lookup?': ['Yes'] * len(final_df)
    }
    summary_df = pd.DataFrame(summary_data)

    # Write the summary DataFrame as the first sheet named 'Metadata'
    summary_df.to_excel(writer, sheet_name='Metadata', index=False)

    # Write each DataFrame to its respective sheet
    for item in final_df:
        # Write DataFrame to a sheet named after the original sheet_name
        item['df'].to_excel(writer, sheet_name=item['sheet_name'], index=False)

print("Excel file has been created with 'Metadata' as the first sheet.")





####################
## Testing
####################            
# Load the updated Excel file into a DataFrame to confirm it saved correctly
loaded_attributes_s7_df = pd.read_excel(output_file_path_attributes, sheet_name='S7 - Attribute')
loaded_attributes_s8_df = pd.read_excel(output_file_path_attributes, sheet_name='S8 - Attribute - Locale')
loaded_lookupdata_df_metadata = pd.read_excel(output_file_path_lookupdata, sheet_name='Metadata')

Expected_57_additions = 58 
Expected_57_deletions = 287
Expected_57_changes = 710
loaded_attributes_s7 = 2609 
Expected_58_additions = 266 
Expected_58_deletions = 1290 
loaded_attributes_s8 = 11387 
expected_items_metadata_lookupdata = 200
loaded_sheets_lookupdata = 201

assert len(attribute_add_s7_set) == Expected_57_additions, f"Expected {Expected_57_additions} additions, got {len(attribute_add_s7_set)}"
assert len(attribute_delete_s7_set) == Expected_57_deletions, f"Expected {Expected_57_deletions} deletions, got {len(attribute_delete_s7_set)}"
assert len(change_set) == Expected_57_changes, f"Expected {Expected_57_changes} changes, got {len(change_set)}"
expected_total_s7 = (len(attribute_add_s7_set) * 2) + len(attribute_delete_s7_set) + len(change_set) * 3
assert expected_total_s7 == 2533, f"Expected 2533 total entries, got {expected_total_s7}"
assert len(loaded_attributes_s7_df) == loaded_attributes_s7, f"Expected {loaded_attributes_s7} total entries in S7, got {len(loaded_attributes_s7_df)}"

assert len(attribute_delete_s8_set) == Expected_58_additions, f"Expected {Expected_58_additions} deletions in S8, got {len(attribute_delete_s8_set)}"
assert len(attribute_add_s8_set) == Expected_58_deletions, f"Expected {Expected_58_deletions} additions in S8, got {len(attribute_add_s8_set)}"
expected_total_s8 = (len(attribute_delete_s8_set) * 4) + (len(attribute_add_s8_set) * 4 * 2)
assert expected_total_s8 == 11384, f"Expected 11384 total entries in S8, got {expected_total_s8}"
assert len(loaded_attributes_s8_df) == loaded_attributes_s8, f"Expected {loaded_attributes_s8} total entries in S8, got {len(loaded_attributes_s8_df)}"

assert len(loaded_lookupdata_df_metadata) == expected_items_metadata_lookupdata, f"Metadata sheet should contain exactly 200 items, found {len(loaded_lookupdata_df_metadata)} items."
expected_sheets_count = loaded_sheets_lookupdata  # 200 data sheets + 1 metadata sheet
actual_sheets_count = len(pd.ExcelFile(output_file_path_lookupdata).sheet_names)
assert actual_sheets_count == expected_sheets_count, f"Excel file should contain {expected_sheets_count} sheets, found {actual_sheets_count} sheets."


print("All tests passed successfully.")
