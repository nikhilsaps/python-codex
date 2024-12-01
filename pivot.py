import pandas as pd
import xlsxwriter

# Function to create the xlsx file with a real Pivot Table
def create_excel_with_real_pivot(csv_file, output_file):
    # Step 1: Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Step 2: Create an Excel writer object with XlsxWriter as the engine
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        # Write the original data to the first sheet
        df.to_excel(writer, sheet_name='Data', index=False)
        
        # Access the workbook and add a worksheet for the pivot table
        workbook  = writer.book
        worksheet = workbook.add_worksheet('Pivot Table')
        
        # Step 3: Create a table range in the 'Data' sheet
        # Create a table range based on the DataFrame shape
        data_range = f"Data!A1:{chr(64 + len(df.columns))}{len(df) + 1}"
        
        # Add a table for the data in the 'Data' sheet
        worksheet = workbook.get_worksheet_by_name('Data')
        worksheet.add_table('A1', f"{chr(64 + len(df.columns))}{len(df) + 1}")

        # Step 4: Create the Pivot Table using the data range
        # Define where the pivot table should be placed
        pivot_range = f"PivotTable!A1"
        
        worksheet = workbook.add_worksheet('Pivot Table')
        
        worksheet.add_pivot_table(
            data_range,
            location=pivot_range,
            row_fields=['login'],  # Rows will be login
            data_fields=['order_id']  # Values will be the count of order_id
        )
    
    print(f"Excel file '{output_file}' created successfully with a Pivot Table!")

# Example usage
csv_file = './assigned.csv'  # Path to your CSV file
output_file = 'output_with_pivot.xlsx'  # Path to output Excel file
create_excel_with_real_pivot(csv_file, output_file)
