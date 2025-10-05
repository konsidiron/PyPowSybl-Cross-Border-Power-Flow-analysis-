import pandas as pd
import os
import pypowsybl.network as pp
import pypowsybl.report as nf
import pypowsybl.loadflow as lf
import logging 

"""
Script that calculates TCC in Romanian/Greek nodes for monthly period of time (Hourly calculations)

"""
# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("TCC_processing.log"),
                              logging.StreamHandler()])

# Function to get user inputs from console
def get_user_inputs():
   
    Path = input("Enter base path for CGM TCC: ex. TCC Folder ")
    Save_folder = input("Enter save folder path: ")
    Year_Month = input("Enter Year_Month (e.g., '202402'): ")
    specific_dates_input = input("Enter specific dates separated by commas ex. 20240212,20240213... (leave blank for all dates): ")
    types = input("Enter types separated by commas (e.g., 'NGR Export,NGR Import,SRO Export,SRO Import'): ")
    
    #Check if user has entered specific types
    if not types.strip():
        types = ['NGR Export', 'NGR Import', 'SRO Export', 'SRO Import']
    else:
        types = types.split(',')   
        
    # Checks if user has entered specific dates
    if specific_dates_input:
        specific_dates = specific_dates_input.split(',') # keeps the list of dates separated by comma
    else:
        specific_dates = None
    
    base_folder = rf'{Path}\\{Year_Month}'
    return types, Year_Month, Save_folder, base_folder, specific_dates


# Function to get all dates from folder (by default specific dates = None) or use specific ones if provided
def get_dates_from_folders(base_folder, specific_dates=None):
    if specific_dates:
        return specific_dates
    
    # Initialize an empty list to store all valid dates (folders) inside the base folder
    all_dates = [folder_name for folder_name in os.listdir(base_folder)
                 if folder_name.isdigit() and len(folder_name) == 8]  # YYYYMMDD format check maybe also the other function check.
    return all_dates

# Function to process the UCTE file and run loadflow
def process_ucte_file(ucte_file_path, Date, current_timestamp, Type):
    try:
        if not os.path.isfile(ucte_file_path):
            print(f"File {ucte_file_path} does not exist. Skipping.")
            return None
        
        #Loads specified UCTE file 
        network = pp.load(ucte_file_path)
        reporter = nf.Reporter()

        #Parameters specification for AC LoadFlow
        p = lf.Parameters(
            distributed_slack=False,    
            transformer_voltage_control_on=False , 
            phase_shifter_regulation_on=True ,
            shunt_compensator_voltage_control_on=True ,
            voltage_init_mode=None, 
            use_reactive_limits=None,  
            twt_split_shunt_admittance=None, 
            read_slack_bus=None, 
            write_slack_bus=None, 
            balance_type=None, 
            dc_use_transformer_ratio=None, 
            countries_to_balance=None, 
            connected_component_mode=None, 
            dc_power_factor=None,
            provider_parameters={
                'maxOuterLoopIterations': str(30),
                'lowImpedanceBranchMode': 'REPLACE_BY_MIN_IMPEDANCE_LINE',}
        )
        lf.run_ac(network, parameters=p, reporter=reporter) # You can use also report_node = reporter 
        print(str(reporter))
        
        # Extract X-nodes and rename certain X-Nodes
        X_nodes = network.get_dangling_lines(attributes=['bus_id', 'boundary_p'])
        X_nodes['bus_id'] = X_nodes['bus_id'].replace({
            'RIS1A41_0': 'RISAC41', 'RMED141_0': 'RMEDG41_0',
            'RPDF241_0': 'RPDFE41', 'RROS241_0': 'RROSI41', 'RTINTA1_0': 'RTINTB1'
        })
        X_nodes['bus_id'] = X_nodes['bus_id'].astype(str)
        X_nodes = X_nodes.dropna(subset=['bus_id'])
        
        # Filter based on the Type
        if Type.startswith('NGR'):
            filtered = X_nodes[X_nodes['bus_id'].str.startswith('G')]
        elif Type.startswith('SRO'):
            filtered = X_nodes[X_nodes['bus_id'].str.startswith('R')]
        else:
            print(f"Warning: Type {Type} does not match expected values for filtering.")
            return None
        
        #Delete unnecessary X-Nodes
        filtered = filtered[~filtered['bus_id'].isin([
            'GARACH1_0', 'RISAC41', 'RARA4D1_0', 'RNADA_1_0', 'RROSI41'
        ])]
        columns_drop = ['id', 'bus_id'] # only p_boundary for TCC
        filtered = filtered.drop(columns=columns_drop, errors='ignore')

        ##Capacity calculation
        tcc_sum = abs(filtered['boundary_p'].sum())
        
        # Return certain dataframe structure 
        return pd.DataFrame({
            'Date': [Date],
            'Timestamp': [current_timestamp],
            'Border & Direction': [Type],
            'TCC': [tcc_sum]
        })
    
    except Exception as e:
        print(f"Error processing file {ucte_file_path}: {e}")
        return None

# Main function to process all data
def process_all_data(base_folder, Year_Month, types, Save_folder, specific_dates=None):
    #Takes dates of specified monthly folder
    dates = get_dates_from_folders(base_folder, specific_dates) 

    #Creates an empty list for the final dataframe
    data = []

    #Iterates through each 'date' folder and through each type folder inside the predefined date folder 
    for Date in dates: 
        for Type in types:
            destination_folder = f'{base_folder}\\{Date}\\CGM\\{Type}'
            
            #Iterates through each UCTE FILE 
            for D in range(11):
                for U in range(11):
                    for i in range(24):
                        time_value = i * 100 + 30
                        base_ucte_filename = f'{Date}_{{:04d}}_2D{D}_UX{U}.uct'
                        current_ucte_filename = base_ucte_filename.format(time_value)
                        current_timestamp = f'{time_value // 100:02d}:30'
                        ucte_file_path = os.path.join(destination_folder, current_ucte_filename)
                        
                        #Saves the structured dataframe from TCC of UCTE file
                        result = process_ucte_file(ucte_file_path, Date, current_timestamp, Type)
                        if result is not None:
                            data.append(result)

    # Concatenate all dataframes and save to Excel
    final = pd.concat(data, ignore_index=True) if data else pd.DataFrame()
    output_file = os.path.join(Save_folder, f'{Year_Month}_TCCS.xlsx')
    final.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")


# Main execution
if __name__ == "__main__":
    # User inforamtion
    types, Year_Month, Save_folder, base_folder, specific_dates = get_user_inputs() 
    #Processing User's info for TCC 
    process_all_data(base_folder, Year_Month, types, Save_folder, specific_dates)
