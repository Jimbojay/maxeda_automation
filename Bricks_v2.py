import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Print the current working directory to confirm the path context
print("Current Directory:", os.getcwd())

# Function to extract individual parent categories from the Parent Category Path
def extract_parents(parent_path):
    # Remove the "GS1//" prefix if it exists
    if parent_path.startswith("GS1//"):
        parent_path = parent_path[len("GS1//"):]
    # Split by "//" to get individual parent categories
    parts = parent_path.split('//')
    # Extract Segment, Family, and Class codes with safe indexing
    return (
        parts[0] if len(parts) > 0 else '',
        parts[1] if len(parts) > 1 else '',
        parts[2] if len(parts) > 2 else ''
    )

def create_category_df(categories, level, sheet):
    print(f'### Create categories at {level} level ###')

    # Initialize an empty DataFrame for categories with specified columns
    columns = sheet.drop(columns=['Segment Code', 'Family Code', 'Class Code']).columns
    Category = pd.DataFrame(columns=columns)
    name_col = level.capitalize() + ' Code'
    path_cols = get_path_cols(level)
    
    # Assign sorted codes to the Category Name column
    Category['Category Name'] = sorted(categories)

    # Define locale settings based on the sheet
    if sheet.equals(maxeda_s9):
        locales = [{'title_prefix': '', 'locale': ''}]
    elif sheet.equals(maxeda_s10):
        locales = [
            {'title_prefix': 'NL ', 'locale': 'nl_NL'},
            {'title_prefix': 'FR ', 'locale': 'fr_FR'}
        ]

    # Process each locale setting
    all_categories = pd.DataFrame()
    for setting in locales:
        _Category = process_locale_setting(Category.copy(), setting, name_col, path_cols, level)
        all_categories = pd.concat([all_categories, _Category], ignore_index=True)

    if sheet.equals(maxeda_s10):
        # Duplicate for 'BE' locales
        be_categories = all_categories.copy()
        be_categories['Locale'] = be_categories['Locale'].replace({'nl_NL': 'nl_BE', 'fr_FR': 'fr_BE'})
        all_categories = pd.concat([all_categories, be_categories], ignore_index=True)

    return all_categories

def get_path_cols(level):
    """Define path columns based on category level."""
    if level == 'brick':
        return ['Segment Code', 'Family Code', 'Class Code']
    elif level == 'family':
        return ['Segment Code']
    elif level == 'class':
        return ['Segment Code', 'Family Code']
    return []

def process_locale_setting(Category, setting, name_col, path_cols, level):
    """Process a single locale setting and return the updated DataFrame."""
    title_col = setting['title_prefix'] + level.capitalize() + ' Title'
    if setting['locale']:
        Category['Locale'] = setting['locale']
    
    Category['Category Long Name'] = Category['Category Name'].map(
        lambda x: f"{x} - {gs1_df.loc[gs1_df[name_col].astype(str) == x, title_col].iloc[0]}"
        if x in gs1_df[name_col].astype(str).values else ''
    )
    
    Category['Parent Category Path'] = Category['Category Name'].apply(
        lambda x: f"GS1//{'//'.join(gs1_df.loc[gs1_df[name_col].astype(str) == x, path_cols].astype(str).iloc[0])}"
        if x in gs1_df[name_col].astype(str).values else 'GS1'
    )

    Category['Hierarchy Name'] = 'GS1 Hierarchy'

    # Duplicate for 'GPC' hierarchy
    Category_dup = Category.copy()
    Category_dup['Category Long Name'] = Category_dup['Category Long Name'].str.split(' - ').str[1]
    Category_dup['Parent Category Path'] = Category_dup['Parent Category Path'].str.replace('GS1//', '').str.replace('GS1', '')
    Category_dup['Hierarchy Name'] = 'GPC'

    return pd.concat([Category, Category_dup], ignore_index=True)
    # elif sheet.equals(maxeda_s10):
    #     title_col = 'NL ' + level.capitalize() + ' Title'
    #     Category['Locale'] = 'nl_NL'

def delete_category_df(categories, sheet):
    print('### Delete categories ###')    
    # Filter the datamodel on the rows that need deletions
    Delete_categories = sheet[sheet['Category Name'].isin(categories)].copy()
    # Delete_categories = Delete_categories[['ID', 'Action', 'Unique Identifier', 'Category Name', 'Category Long Name', 'Parent Category Path', 'Hierarchy Name']]

    # Substitute nan by blank
    Delete_categories.replace('nan', '', inplace=True)

    # Add the delete action
    Delete_categories['Action'] = 'Delete'
    
    # # Apply function to determine the level based on 'Parent Category Path'
    # def get_level_delete(parent_path):
    #     slash_count = parent_path.count('//')
    #     if slash_count == 2:
    #         return 'brick'
    #     elif slash_count == 1:
    #         return 'class'
    #     elif slash_count == 0:
    #         return 'family'
    #     elif slash_count == 0:
    #         return 'segment'
    #     return 'unknown'  # Default case if none match

    # Create a new column 'Level' based on the slash count
    # Delete_categories['Origin'] = Delete_categories['Parent Category Path'].apply(get_level_delete).apply(lambda x: f"delete {x}")

    return Delete_categories

# Set file paths and read the specified sheets
gs1_file_path = os.getenv('path_datamodel_GS1')
datamodel_file_path = os.getenv('path_datamodel_maxeda')

###################
## GS1 datamodel
###################
print('### Read GS1 datamodel ###')
# Read the 'Bricks' sheet from the GS1 file, starting from row 4 for headers
gs1_df = pd.read_excel(gs1_file_path, sheet_name='Bricks', skiprows=3, dtype=str)
# Select relevant columns
gs1_df = gs1_df[['Brick Code','Brick activated', 'Brick Title', 'Segment Code', 'Segment Title', 'Family Code', 'Family Title', 'Class Code', 'Class Title', 'FR Brick Title', 'FR Segment Title', 'FR Family Title', 'FR Class Title','NL Brick Title', 'NL Segment Title', 'NL Family Title', 'NL Class Title']].astype(str)


###################
## Maxeda datamodel
###################
def maxeda_sheet(sheet):
    print(f'### Read Maxeda datamodel {sheet} ###')
    # Read the 'S9 - Category' sheet from the Datamodel file
    maxeda_sheet = pd.read_excel(datamodel_file_path, sheet_name=sheet, dtype=str)
    # Select relevant columns
    maxeda_sheet = maxeda_sheet.astype(str).applymap(lambda x: x.strip("'\""))

    # Apply the function to extract Segment, Family, and Class Codes from the datamodel_bricks
    print('### Extract parents ###')
    maxeda_sheet[['Segment Code', 'Family Code', 'Class Code']] = maxeda_sheet['Parent Category Path'].apply(extract_parents).apply(pd.Series)

    return maxeda_sheet

maxeda_s9 = maxeda_sheet('S9 - Category')
maxeda_s10 = maxeda_sheet('S10 - Category - Locale')


# Create a new DataFrame using the structure of 'S9 - Category'
print('### Construct S9-Categories ###')

# Category = pd.DataFrame(columns=maxeda_s9.columns)

#####################
## Add
#####################
print('### Additions ###')
# Convert data to sets for comparison of just the Category Names
# print(gs1_df)
gs1_df_active= gs1_df[gs1_df['Brick activated'] == 'Yes']
gs1_active_bricks_set = set(gs1_df_active['Brick Code'].dropna())
gs1_active_segments_set = set(gs1_df_active['Segment Code'].dropna())
gs1_active_families_set = set(gs1_df_active['Family Code'].dropna())
gs1_active_classes_set = set(gs1_df_active['Class Code'].dropna())


def sheet(sheet_df):
    all_categories = []

    # Filter the DataFrame to include only rows where 'Hierarchy Name' is 'GS1 Hierarchy'
    base_fiter_datamodel = sheet_df[(sheet_df['Hierarchy Name'].isin(['GS1 Hierarchy']))]

    # Convert datamodel to sets
    filtered_datamodel_bricks = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 3)]
    datamodel_bricks_set = set(filtered_datamodel_bricks['Category Name'].dropna())

    filtered_datamodel_segments = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 0)]
    datamodel_segments_set = set(filtered_datamodel_segments['Category Name'].dropna())

    filtered_datamodel_families = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 1)]
    datamodel_families_set = set(filtered_datamodel_families['Category Name'].dropna())

    filtered_datamodel_classes = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 2)]
    datamodel_classes_set = set(filtered_datamodel_classes['Category Name'].dropna())

    # Create a list to store data for new categories
    levels_and_data_new = [
        ('brick', gs1_active_bricks_set - datamodel_bricks_set),
        ('segment', gs1_active_segments_set - datamodel_segments_set),
        ('family', gs1_active_families_set - datamodel_families_set),
        ('class', gs1_active_classes_set - datamodel_classes_set)
    ]

    # Loop through each level to generate corresponding DataFrames
    for level, selection in levels_and_data_new:
        new_categories = create_category_df(selection, level, sheet_df)
        all_categories.append(new_categories)


    #####################
    ## Delete
    #####################
    print('### Deletions ###')
    gs1_bricks_set = set(gs1_df['Brick Code'].dropna())
    gs1_segments_set = set(gs1_df['Segment Code'].dropna())
    gs1_families_set = set(gs1_df['Family Code'].dropna())
    gs1_classes_set = set(gs1_df['Class Code'].dropna())

    # Initialize an empty set to hold the combined results
    delete_set = set()

    # Create a list to store data for new categories
    levels_and_data_delete = [
        ('brick', datamodel_bricks_set - gs1_bricks_set),
        ('segment', datamodel_segments_set - gs1_segments_set),
        ('family', datamodel_families_set - gs1_families_set),
        ('class', datamodel_classes_set - gs1_classes_set)
    ]

    # Loop through the levels and update the combined set with each difference set
    for level, difference_set in levels_and_data_delete:
        delete_set.update(difference_set)

    delete_categories = delete_category_df(delete_set, sheet_df)
    # Filter out rows where 'Category Name' starts with '999'
    delete_categories = delete_categories[~delete_categories['Category Name'].str.startswith('999')]

    all_categories.append(delete_categories)

    ######################
    ## Change in hierachy of active bricks
    #####################
    print('### Hierarchy change ###')
    # Merge the two dataframes on Brick Code
    comparison_df_hierarchy = pd.merge(gs1_df_active, sheet_df, how='inner', left_on='Brick Code', right_on='Category Name')

    # Check for mismatched parents using trimmed and type-consistent comparisons
    change_hierarchy_df = comparison_df_hierarchy[
        (comparison_df_hierarchy['Segment Code_x'] != comparison_df_hierarchy['Segment Code_y']) |
        (comparison_df_hierarchy['Family Code_x'] != comparison_df_hierarchy['Family Code_y']) |
        (comparison_df_hierarchy['Class Code_x'] != comparison_df_hierarchy['Class Code_y'])
    ]

    change_hierarchy_set = set(change_hierarchy_df['Brick Code'].dropna())

    # print('change hierarchy')
    # print(change_hierarchy_set)

    delete_old_hierarchy = delete_category_df(change_hierarchy_set, sheet_df)
    create_new_hierarchy = create_category_df(change_hierarchy_set, 'brick', sheet_df)

    all_categories.append(delete_old_hierarchy)
    all_categories.append(create_new_hierarchy)

    # Concatenate all DataFrames into one for S9 - Category, if there are any DataFrames to concatenate
    if all_categories:
        final_all_categories = pd.concat(all_categories, ignore_index=True)
    else:
        final_all_categories = pd.DataFrame()  # Fallback to an empty DataFrame if no data


    return final_all_categories

    

###################
## Write output
###################
print('### Output ###')

final_all_categories_s9 = sheet(maxeda_s9)
final_all_categories_s9 = final_all_categories_s9[['ID', 'Action', 'Unique Identifier', 'Category Name', 'Category Long Name', 'Parent Category Path', 'Hierarchy Name']]

final_all_categories_locale_combined = sheet(maxeda_s10)    
final_all_categories_locale_combined = final_all_categories_locale_combined[['ID', 'Action', 'Unique Identifier', 'Category Name', 'Parent Category Path', 'Hierarchy Name', 'Locale', 'Category Long Name']]


print(final_all_categories_locale_combined)

# Write the concatenated DataFrame to an Excel file
output_file_path = os.path.join(os.getcwd(), 'GS1_vs_Datamodel_Comparison_Bricks.xlsx')

with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    final_all_categories_s9.to_excel(writer, sheet_name='S9 - Category', index=False)
    final_all_categories_locale_combined.to_excel(writer, sheet_name='S10 - Category - Locale', index=False)
            
# Load the updated Excel file into a DataFrame
comparison_s9 = pd.read_excel(output_file_path, sheet_name='S9 - Category')
print(len(comparison_s9))
   
comparison_s10 = pd.read_excel(output_file_path, sheet_name='S10 - Category - Locale')
print(len(comparison_s10))