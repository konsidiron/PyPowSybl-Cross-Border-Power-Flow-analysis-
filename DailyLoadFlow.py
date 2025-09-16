import pypowsybl.network as pp
import pypowsybl.loadflow as lf 
import pandas as pd
import os 
import logging 
import pypowsybl.report as nf

# Set up logging
logging.basicConfig(level=logging.INFO)

def get_user_inputs():
    # Get the folder paths
    ucte_folder = input("Enter the path to your UCTE files folder: ")
    output_folder = input("Enter the path where output reports should be saved: ")

    # Get the date, file type, country code, and format
    date = input("Enter the date (e.g., 20240717): ")
    file_type = input("Enter the file type (e.g., FO3): ")
    country_code = input("Enter the country code (e.g., GR): ")
    format = input("Enter the format (e.g., UCT): ")

    # Define hours or ask user for specific hours
    hours = input("Enter the hours (comma-separated ex. 0030,0130 ... , or leave blank for default 0030-2330): ")
    if not hours:
        hours = ['0030', '0130', '0230', '0330', '0430', '0530', '0630', '0730', '0830', '0930', '1030', '1130',
                 '1230', '1330', '1430', '1530', '1630', '1730', '1830', '1930', '2030', '2130', '2230', '2330']
    else:
        hours = hours.split(',')

    # Get the number range or use default
    numbers = input("Enter the version numbers (comma-separated ex. 0,8... , or leave blank for default 0-9): ")
    if not numbers:
        numbers = range(0, 10)
    else:
        numbers = [int(x) for x in numbers.split(',')]

    return ucte_folder, output_folder, date, file_type, country_code, format, hours, numbers

#Adjust prefixes for I values based on P,Q.
def adjust_prefixes(df):         
    def has_minus(value):
        return '-' in str(value)

    df['P_minus'] = df['P'].apply(lambda x: '-' if has_minus(x) else '')
    df['Q_minus'] = df['Q'].apply(lambda x: '-' if has_minus(x) else '')
    
    df['I'] = df.apply(
        lambda row: f"{row['P_minus']}{row['I']}" if row['P'] != 0 and row['P_minus'] else 
                    f"{row['Q_minus']}{row['I']}" if row['P'] == 0 and row['Q_minus'] else 
                    row['I'],
        axis=1
    )
    
    df.drop(columns=['P_minus', 'Q_minus'], inplace=True)
    return df

#Convert I values to numeric
def convert_to_numeric(df):
    df['I'] = pd.to_numeric(df['I'], errors='coerce')
    return df

#Uses above functions at once
def process_df(df):
    df = adjust_prefixes(df)
    df = convert_to_numeric(df)
    return df

def process_network_files_from_user_inputs():
    # Get user inputs
    ucte_folder, output_folder, date, file_type, country_code, format, hours, numbers = get_user_inputs()

    # Process network files using user-defined inputs
    process_network_files(date, hours, numbers, file_type, country_code, format, ucte_folder, output_folder)

def process_network_files(date, hours, numbers, file_type, country_code, format, ucte_folder, output_folder):
    #Loop through the hours and find for each hour highest version using find_highest_version_file
    for hour in hours:
        highest_number, selected_ucte_path = find_highest_version_file(date, hour, numbers, file_type, country_code, format, ucte_folder)
        
        if selected_ucte_path:
            logging.info(f"Highest number version for {hour}: {highest_number}")
            process_and_save_network(selected_ucte_path, date, hour, file_type, country_code, output_folder)
        else:
            logging.warning(f"No valid UCTE file found for {hour}.")


def find_highest_version_file(date, hour, numbers, file_type, country_code, format, ucte_folder):
    highest_number = -1
    selected_ucte_path = None

    for number in numbers:
        ucte_filename = f'{date}_{hour}_{file_type}_{country_code}{number}.{format}'
        ucte_path = os.path.join(ucte_folder, ucte_filename)

        if os.path.exists(ucte_path):
            if number > highest_number:
                highest_number = number
                selected_ucte_path = ucte_path
        
    return highest_number, selected_ucte_path

def process_and_save_network(ucte_path ,date, hour, file_type, country_code, output_folder):
    # Load network
    network = pp.load(ucte_path)
    reporter = nf.Reporter()

    # Define load flow parameters
    p = lf.Parameters(
        distributed_slack=False ,
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
        'maxOuterLoopIterations' : str(30) ,
        'lowImpedanceBranchMode ': 'REPLACE_BY_MIN_IMPEDANCE_LINE' ,
        'slackBusesIds'  : 'G5MEGA14' 
    }) 

    # Run loadflow
    lf.run_ac(network, parameters=p, reporter=reporter)
    print(str(reporter))
    logging.info(f"LoadFlow completed for {hour}.") 
     
    # Process DataFrames for different components
    nodes = process_bus_sheet(network)
    current_limits = process_current_limits(network)
    lines_final = process_lines(network, nodes, current_limits)
    transformers = process_transformers(network, nodes, current_limits)
    x_nodes = process_x_nodes(network, nodes, current_limits)
    switches = process_switches(network)

    # Define output file name
    output_filename = f'{date}_{hour}_{file_type}_{country_code}_0_OPENLF_REPORT.xlsx'
    output_path = os.path.join(output_folder, output_filename)

    # Save to Excel
    save_to_excel(output_path, nodes, transformers, lines_final, x_nodes, switches)

def save_to_excel(output_path, nodes, transformers, lines_final , x_nodes, switches):
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        nodes.to_excel(writer, sheet_name='Bus', index=False)
        transformers.to_excel(writer, sheet_name='Transformers', index=False)
        lines_final.to_excel(writer, sheet_name='Line', index=False)
        x_nodes.to_excel(writer, sheet_name='X-Nodes', index=False)
        switches.to_excel(writer, sheet_name='Switches', index=False)


def process_bus_sheet(network):

    #BUS attributes selection
    buses = network.get_bus_breaker_view_buses(attributes=['v_mag', 'v_angle'])
    loads = network.get_loads(attributes=['p', 'q'])
    generators = network.get_generators(attributes=['target_v', 'p', 'q', 'max_q', 'min_q', 'voltage_regulator_on'])

    #Reset indices and renaame columns for merging 
    buses.reset_index(inplace=True)
    buses.rename(columns={'index': 'id'}, inplace= True)
    loads.reset_index(inplace=True)
    loads.rename(columns={'index': 'id'}, inplace= True)
    generators.reset_index(inplace=True)
    generators.rename(columns={'index': 'id'}, inplace= True)
    
    # Validate that 'id' column exists in all dataframes before proceeding
    for df, name in zip([generators, loads, buses], ["generators", "loads", "buses"]):
        if 'id' not in df.columns:
            raise ValueError(f"'{name}' DataFrame is missing the 'id' column.")

    # Create 'id_8' column by extracting the first 8 characters of the 'id' column
    for df in [generators, loads, buses]:
        df['id_8'] = df['id'].astype(str).str[:8]

    # Merge DataFrames
    nodes = pd.merge(buses, generators, on='id_8', suffixes=('_bus', '_gen'), how='outer')
    nodes = pd.merge(nodes, loads, on='id_8', suffixes=('', '_load'), how='outer')
    
    #Fill empty cells and rename columns
    nodes.drop(columns=['id_8', 'id_gen' , 'id' ], inplace=True)
    columns = ['p' , 'q' , 'p_load' , 'q_load']
    nodes[columns] = nodes[columns].fillna(0) # Drop zero values to empty cells
    rename_columns = {
        'target_v': 'Reference Voltage',
        'p': 'Pgen',
        'q': 'Qgen',
        'p_load': 'Pload',
        'q_load': 'Qload',
        'id_bus' : 'BUS'
    }
    nodes.rename(columns=rename_columns, inplace=True)

    return nodes

def process_current_limits(network):
    current_limits = network.get_operational_limits()
    current_limits.reset_index(inplace=True)
    current_limits.rename(columns={'index': 'element_id'}, inplace=True)
    return current_limits

def process_lines(network, nodes, current_limits):
    lines = network.get_lines(attributes=['bus_breaker_bus1_id', 'i1', 'p1', 'q1', 'i2', 'p2', 'q2', 'bus_breaker_bus2_id'])
    lines.reset_index(inplace=True)
    lines.rename(columns={'index': 'id'}, inplace=True)

    # Create DataFrame for '1' side attributes
    lines_1_df = lines[['id', 'i1', 'p1', 'q1' ,'bus_breaker_bus1_id' ]].copy()
    lines_1_df.rename(columns={'i1': 'I', 'p1': 'P', 'q1': 'Q' , 'bus_breaker_bus1_id' : 'BUS'}, inplace=True)
    lines_1_df['side'] = 1

    # Create DataFrame for '2' side attributes
    lines_2_df = lines[['id', 'i2', 'p2', 'q2' , 'bus_breaker_bus2_id' ]].copy()
    lines_2_df.rename(columns={'i2': 'I', 'p2': 'P', 'q2': 'Q' , 'bus_breaker_bus2_id' :'BUS' }, inplace=True)
    lines_2_df['side'] = 2

    # Combine both DataFrames and merge with operational limits
    reduced_operational_limits_df = current_limits[(current_limits['side'] == 'ONE') | (current_limits['side'] == 'TWO')]
    merged_lines_1_df = pd.merge(lines_1_df, reduced_operational_limits_df, left_on='id', right_on='element_id', how='left')
    merged_lines_2_df = pd.merge(lines_2_df, reduced_operational_limits_df, left_on='id', right_on='element_id', how='left')
  
    #Drop unnecessary columns
    merged_lines_1_df.drop(columns=['element_id' , 'element_type' , 'side_y' , 'name' , 'type' , 'acceptable_duration' ], inplace=True)
    merged_lines_2_df.drop(columns=['element_id' , 'element_type' , 'side_y' , 'name' , 'type' , 'acceptable_duration'], inplace=True)

    #Add prefixes, numeric form
    merged_lines_1_df['id'] = merged_lines_1_df['id'].astype(str).str.replace(' ', '_')
    merged_lines_2_df['id'] = merged_lines_2_df['id'].astype(str).str.replace(' ', '_')
    merged_lines_1_df = process_df(merged_lines_1_df)
    merged_lines_2_df = process_df(merged_lines_2_df)

    #Concatenate, sort and rename
    combined_lines_df = pd.concat([merged_lines_1_df, merged_lines_2_df], ignore_index=True)
    combined_lines_df.drop_duplicates(inplace=True) 
    combined_lines_df.sort_values(by=['id', 'side_x'], ascending=[True, True], inplace=True)
    combined_lines_df.reset_index(drop=True, inplace=True)
    columns_L = ['I' , 'P' , 'Q']
    combined_lines_df[columns_L] = combined_lines_df[columns_L].fillna(0) # Drop zero values to empty cells
    rename_columns = {
    'value' : 'I_limit'
    }
    combined_lines_df.rename(columns = rename_columns , inplace = True)

    ##Add voltage magnitude and theta to both sides of the line
    voltage_theta = nodes[['BUS', 'v_mag', 'v_angle']]
    lines_final = pd.merge(voltage_theta , combined_lines_df , on='BUS', how='left')
    columns_to_check = ['I', 'P', 'Q', 'side_x', 'I_limit']
    mask = lines_final[columns_to_check].isna().all(axis=1)
    lines_final = lines_final[~mask]
    Order = ['id', 'side_x', 'BUS', 'v_mag', 'v_angle', 'I', 'I_limit', 'P', 'Q']
    lines_final = lines_final[Order]

    return lines_final

def process_transformers(network, nodes, current_limits):
   #Attributes selection
   transformers = network.get_2_windings_transformers(attributes =['rated_u1' , 'rated_u2' , 'bus_breaker_bus1_id' , 'p1' , 'q1' , 'i1', 'p2' , 'q2' , 'i2' , 'bus_breaker_bus2_id'] )
   transformers.reset_index(inplace=True)
   transformers.rename(columns={'index': 'id'}, inplace=True)

   #Create dataframe for '1', '2' side attributes
   transformer_1_df = transformers[[ 'id','i1', 'p1', 'q1' ,'bus_breaker_bus1_id' , 'rated_u1' ]].copy()
   transformer_1_df.rename(columns={'i1': 'I', 'p1': 'P', 'q1': 'Q' , 'bus_breaker_bus1_id' : 'BUS' , 'rated_u1' : 'Base Voltage'}, inplace=True)
   transformer_1_df['side'] = 2
 
   transformer_2_df = transformers[[ 'id','i2', 'p2', 'q2' , 'bus_breaker_bus2_id' , 'rated_u2' ]].copy()
   transformer_2_df.rename(columns={'i2': 'I', 'p2': 'P', 'q2': 'Q' , 'bus_breaker_bus2_id' :'BUS' , 'rated_u2' : 'Base Voltage'}, inplace=True)
   transformer_2_df['side'] = 1
    
   #Merge dataframes with operational limits and drop unnecessary columns
   transformer_1_df = pd.merge(transformer_1_df, current_limits, left_on='id', right_on='element_id', how='left')
   transformer_2_df = pd.merge(transformer_2_df, current_limits, left_on='id', right_on='element_id', how='left')
   transformer_1_df.drop(columns=['element_id' , 'element_type' , 'side_y' , 'name' , 'type' , 'acceptable_duration' ], inplace=True)
   transformer_2_df.drop(columns=['element_id' , 'element_type' , 'side_y' , 'name' , 'type' , 'acceptable_duration'], inplace=True)
 
   #Add prefix, numeric form and replace ' ' with '_'
   transformer_1_df['id'] = transformer_1_df['id'].astype(str).str.replace(' ', '_')
   transformer_2_df['id'] = transformer_2_df['id'].astype(str).str.replace(' ', '_')
   transformer_1_df = process_df(transformer_1_df)
   transformer_2_df = process_df(transformer_2_df)

   #Concatenate sides and sort
   Transformers = pd.concat([transformer_1_df, transformer_2_df], ignore_index=True)
   Transformers.drop_duplicates(inplace=True)
   Transformers.sort_values(by=['id', 'side_x'], ascending=[True, True], inplace=True)
   Transformers.reset_index(drop=True, inplace=True)
   columns_L = ['I' , 'P' , 'Q']
   Transformers[columns_L] = Transformers[columns_L].fillna(0)
   rename_columns = {
   'value' : 'I_limit'
 }
   Transformers.rename(columns = rename_columns , inplace = True)

   #Add voltage magnitude and theta to each side of the transformer
   filtered_merged_df_transformer = nodes[['BUS', 'v_mag', 'v_angle']]
   transformers = pd.merge(filtered_merged_df_transformer , Transformers , on='BUS', how='left')
   transformers = transformers[transformers['I'] != 0]
   transformers.reset_index(drop=True, inplace=True)
   columns_to_check = ['I', 'P', 'Q', 'side_x', 'I_limit']
   mask = transformers[columns_to_check].isna().all(axis=1)
   transformers = transformers[~mask]
   Order = ['id', 'side_x', 'BUS', 'Base Voltage' , 'v_mag', 'v_angle', 'I', 'I_limit', 'P', 'Q']
   transformers = transformers[Order]

   return transformers

def process_x_nodes(network, nodes, current_limits):
    #Attributes selection
    X_nodes_lines = network.get_dangling_lines(attributes = [ 'bus_breaker_bus_id', 'i' , 'p' , 'q','boundary_v_mag' , 'boundary_v_angle', 'boundary_p', 'boundary_q'])
    X_nodes_lines.reset_index(inplace=True)
    X_nodes_lines.rename(columns={'index': 'id'}, inplace=True)

    #Merge with current limits and drop, rename columns
    x_nodes = pd.merge(X_nodes_lines , current_limits, left_on='id', right_on='element_id', how='left' )
    x_nodes['id'] = x_nodes['id'].astype(str).str.replace(' ', '_')
    x_nodes.drop(columns=['element_id' , 'element_type' , 'side' , 'name' , 'type' , 'acceptable_duration' ], inplace=True)
    x_nodes.rename(columns={'i': 'I', 'p': 'P', 'q': 'Q' , 'value' : 'I_limit' , 'bus_breaker_bus_id' : 'BUS'}, inplace=True)

    #Add prefix to I, numeric form and fill 0 values to empty cells
    x_nodes = process_df(x_nodes)
    columns_X = ['I' , 'P' , 'Q' , 'boundary_v_mag' , 'boundary_v_angle' , 'boundary_p' , 'boundary_q' ]
    x_nodes[columns_X] = x_nodes[columns_X].fillna(0)

    #Add voltage magnitude and theta to X-Nodes side.
    merge_X_nodes = nodes[['BUS', 'v_mag', 'v_angle']]
    X_Nodes = pd.merge( merge_X_nodes , x_nodes , on='BUS', how='left')
    columns_to_check = ['I', 'P', 'Q', 'I_limit']
    mask = X_Nodes[columns_to_check].isna().all(axis=1)
    X_Nodes = X_Nodes[~mask]
    Order = ['id', 'BUS', 'v_mag', 'v_angle', 'I', 'I_limit', 'P', 'Q' , 'boundary_v_mag' , 'boundary_v_angle' , 'boundary_p' , 'boundary_q' ]
    X_Nodes = X_Nodes[Order]

    return X_Nodes

def process_switches(network):
    switches = network.get_switches(attributes=['bus_breaker_bus1_id', 'kind', 'open', 'retained', 'bus_breaker_bus2_id'])
    switches.reset_index(inplace=True)
    switches.rename(columns={'index': 'id'}, inplace=True)
    return switches


if __name__ == "__main__":
    process_network_files_from_user_inputs()


