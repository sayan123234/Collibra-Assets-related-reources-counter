import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from optimized_counts import get_all_counts
from get_all_assets import get_all_assets
from OauthAuth import oauth_bearer_token
from get_assetType_name import get_asset_type_name

# Load environment variables
load_dotenv(override=True)

def load_asset_type_ids(filepath: str = "Collibra_Asset_Type_Id_Manager.json") -> list:
    """
    Load asset type IDs from JSON file.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get('ids', [])
    except Exception as e:
        print(f"Error loading asset type IDs: {e}")
        return []

def get_base_url() -> str:
    """
    Get base URL from environment variables.
    """
    instance_url = os.getenv('COLLIBRA_INSTANCE_URL')
    if not instance_url:
        raise ValueError("COLLIBRA_INSTANCE_URL not set in environment variables")
    return f"https://{instance_url}"

def process_asset_type(asset_type_id: str, asset_type_name: str, base_url: str, bearer_token: str) -> pd.DataFrame:
    """
    Process a single asset type and return its data as a DataFrame.
    """
    print(f"\nProcessing asset type: {asset_type_name} ({asset_type_id})")
    
    # Get asset IDs
    print("Fetching asset IDs...")
    asset_ids = get_all_assets(asset_type_id, base_url, bearer_token)
    
    if not asset_ids:
        print("No assets found for this asset type.")
        return pd.DataFrame()
        
    print(f"Found {len(asset_ids)} assets. Fetching details...")
    
    # Get all counts concurrently
    counts = get_all_counts(asset_ids, base_url, bearer_token)
    
    # Create DataFrame
    data = []
    for asset_id in asset_ids:
        asset_counts = counts.get(asset_id, {})
        data.append({
            'assetId': asset_id,
            'assetTypeName': asset_type_name,
            'assetTypeId': asset_type_id,
            'attributeCount': asset_counts.get('attributes', 0),
            'incomingRelationCount': asset_counts.get('incoming', 0),
            'outgoingRelationCount': asset_counts.get('outgoing', 0),
            'responsibilitiesRelationCount': asset_counts.get('responsibilities', 0)
        })
    
    return pd.DataFrame(data)

def save_to_excel(dataframes: dict, output_file: str):
    """
    Save data to Excel with multiple sheets and formatting.
    """
    print(f"\nSaving data to Excel: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = []
        total_assets = 0
        
        # Write each asset type to its own sheet and collect summary data
        for asset_type_name, df in dataframes.items():
            if not df.empty:
                # Write the data
                df.to_excel(writer, sheet_name=asset_type_name[:31], index=False)  # Excel sheet names limited to 31 chars
                
                # Access the worksheet
                worksheet = writer.sheets[asset_type_name[:31]]
                
                # Format headers
                for col in range(1, df.shape[1] + 1):
                    cell = worksheet.cell(row=1, column=col)
                    cell.style = 'Headline 3'
                
                # Adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column = list(column)
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
                
                # Collect summary data
                asset_count = len(df)
                total_assets += asset_count
                total_attrs = df['attributeCount'].sum()
                total_relations = df['incomingRelationCount'].sum() + df['outgoingRelationCount'].sum()
                total_responsibilities = df['responsibilitiesRelationCount'].sum()
                
                summary_data.append({
                    'Asset Type': asset_type_name,
                    'Asset Type ID': df['assetTypeId'].iloc[0],
                    'Total Assets': asset_count,
                    'Total Attributes': total_attrs,
                    'Total Relations': total_relations,
                    'Total Responsibilities': total_responsibilities
                })
        
        # Create and write summary sheet
        summary_df = pd.DataFrame(summary_data)
        if not summary_df.empty:
            # Add totals row
            totals = {
                'Asset Type': 'TOTAL',
                'Asset Type ID': '',
                'Total Assets': summary_df['Total Assets'].sum(),
                'Total Attributes': summary_df['Total Attributes'].sum(),
                'Total Relations': summary_df['Total Relations'].sum(),
                'Total Responsibilities': summary_df['Total Responsibilities'].sum()
            }
            summary_df = pd.concat([summary_df, pd.DataFrame([totals])], ignore_index=True)
            
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format summary sheet
            worksheet = writer.sheets['Summary']
            
            # Format headers
            for col in range(1, summary_df.shape[1] + 1):
                cell = worksheet.cell(row=1, column=col)
                cell.style = 'Headline 2'
            
            # Format totals row
            for col in range(1, summary_df.shape[1] + 1):
                cell = worksheet.cell(row=summary_df.shape[0], column=col)
                cell.style = 'Total'
            
            # Adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column = list(column)
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

def main():
    try:
        # Get base URL from environment
        base_url = get_base_url()
        
        # Get OAuth token
        bearer_token = oauth_bearer_token()
        if not bearer_token:
            raise ValueError("Failed to obtain OAuth token")
        
        # Load asset type IDs
        asset_type_ids = load_asset_type_ids()
        if not asset_type_ids:
            raise ValueError("No asset type IDs found in configuration file")
        
        # Process each asset type
        dataframes = {}
        for asset_type_id in asset_type_ids:
            # Get asset type name
            asset_type_name = get_asset_type_name(asset_type_id)
            if not asset_type_name:
                print(f"Warning: Could not get name for asset type {asset_type_id}, using ID instead")
                asset_type_name = f"AssetType_{asset_type_id[:8]}"
            
            df = process_asset_type(asset_type_id, asset_type_name, base_url, bearer_token)
            if not df.empty:
                dataframes[asset_type_name] = df
        
        if not dataframes:
            print("No data found for any asset type.")
            return
            
        # Create output directory if it doesn't exist
        output_dir = os.getenv('FILE_SAVE_LOCATION', 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_format = os.getenv('OUTPUT_FORMAT', 'csv').lower()
        
        # Set correct file extension based on format
        if output_format == 'excel':
            file_extension = 'xlsx'
        else:
            file_extension = output_format
            
        output_file = os.path.join(output_dir, f'asset_counts_{timestamp}.{file_extension}')
        
        if output_format == 'excel':
            save_to_excel(dataframes, output_file)
        else:
            # Combine all dataframes for non-Excel formats
            final_df = pd.concat(dataframes.values(), ignore_index=True)
            if output_format == 'csv':
                final_df.to_csv(output_file, index=False)
            elif output_format == 'json':
                final_df.to_json(output_file, orient='records')
            else:
                print(f"Unsupported output format: {output_format}. Defaulting to CSV.")
                output_file = os.path.join(output_dir, f'asset_counts_{timestamp}.csv')
                final_df.to_csv(output_file, index=False)
            
        print(f"\nResults saved to {os.path.abspath(output_file)}")
        
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()