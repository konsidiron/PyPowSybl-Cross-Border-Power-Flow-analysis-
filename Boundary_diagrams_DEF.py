import matplotlib.pyplot as plt
import pypowsybl.network as pp
import pypowsybl.loadflow as lf 
import pandas as pd
import pypowsybl.report as nf
import math
import os 

def get_user_inputs():
    """
    Get user inputs for folder paths, date, file type, country code, and numbers.
    """
    ucte_folder = input("Enter the path for UCTE folder (e.g., 'C:/Users/k.sidiropoulos/Downloads/CGM_greek_nodes'): ").strip()
    output_folder = input("Enter the path for Excel output folder (e.g., 'C:/Users/k.sidiropoulos/Downloads/CGM_greek_nodes/Daily_excel'): ").strip()
    output_folder1 = input("Enter the path for diagrams output folder (e.g., 'C:/Users/k.sidiropoulos/Downloads/CGM_greek_nodes/Daily_excel/diagrams'): ").strip()
    Date = input("Enter the date in YYYYMMDD format (e.g., '20240717'): ").strip()
    File_type = input("Enter the file type (e.g., 'FO3'): ").strip()
    country_code = input("Enter the country code (e.g., 'UX'): ").strip()
    format = input("Enter the file format (e.g., 'UCT'): ").strip()
    numbers = input("Enter the range of numbers (e.g., '0-20'): ").strip()
    
    # Convert 'numbers' input to a range
    try:
        start, end = map(int, numbers.split('-'))
        numbers = range(start, end + 1)
    except ValueError:
        print("Invalid range input. Using default range 0-20.")
        numbers = range(0, 21)

    # Return user inputs
    return ucte_folder, output_folder, output_folder1, Date, File_type, country_code, format, numbers

def main():
    # Get user inputs
    ucte_folder, output_folder, output_folder1, Date, File_type, country_code, format, numbers = get_user_inputs()
    #Timestamps
    hours = ['0030', '0130', '0230', '0330', '0430', '0530', '0630', '0730', '0830', '0930', '1030', '1130', 
         '1230', '1330', '1430', '1530', '1630', '1730', '1830', '1930', '2030', '2130', '2230', '2330']
    combined = pd.DataFrame()  # Initialize combined DataFrame 
    #Iterate through each timestamp
    for hour in hours:
        highest_number = -1
        selected_ucte_path = None
        #Iterate through numbers to check for highest UCTE version
        for number in numbers:
            # Construct the UCTE filename
            ucte_filename = f'{Date}_{hour}_{File_type}_{country_code}{number}.{format}'
            ucte_path = os.path.join(ucte_folder, ucte_filename)

            if os.path.exists(ucte_path):
                if number > highest_number:
                    highest_number = number
                    selected_ucte_path = ucte_path

        if selected_ucte_path:
            print(f'Highest number version was: {highest_number}')
            network = load_and_run_loadflow(selected_ucte_path)
            filtered_data = extract_boundary_nodes(network, hour)
            combined = pd.concat([combined, filtered_data], ignore_index=True)
        else:
            print(f'No valid version found for hour: {hour}. Skipping this hour.')
            continue  # Skip this hour and move on to the next one

    if not combined.empty:
        output_file = os.path.join(output_folder, f'GREEK_BOUNDARY_NODES_{Date}.xlsx')
        save_combined_data(combined, output_file)
        generate_plots(hours, output_file, output_folder1)
        print("Current, active power, and reactive power plotting completed. Files are saved to the output folder.")
    else:
        print("No valid data was processed. No output generated.")

def load_and_run_loadflow(selected_ucte_path):
        """
          Load the network from the UCTE file and run the AC load flow.
        """
        #Load the UCTE 
        network = pp.load(selected_ucte_path)
        reporter = nf.Reporter()
        #PERFORMING AC LOADFLOW, specifying the parameters
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
            'lowImpedanceBranchMode': 'REPLACE_BY_MIN_IMPEDANCE_LINE' 
                })
        lf.run_ac(network , parameters = p , reporter= reporter) # User can use report_node = reporter instead reporter = reporter. 
        #Print the provider parameters
        print(str(reporter))

        return network
        

def extract_boundary_nodes(network, hour):
    """
    Extract X-Nodes (boundary nodes) from the network and filter Greek boundary nodes.
    """
    #Get the X-nodes of the UCTE
    X_nodes = network.get_dangling_lines(attributes = ['bus_breaker_bus_id' ,'i' , 'p' , 'q' ])  # TAKE ALL THE BOUNDARY LINES OF THE CGM 
    X_nodes.reset_index(inplace=True)
    X_nodes.rename(columns={'index': 'id'}, inplace=True)
    X_nodes['id'] = X_nodes['id'].astype(str).str.replace(' ', '_')
    X_nodes.rename(columns={'i': 'I', 'p': 'P', 'q': 'Q' , 'bus_breaker_bus_id': 'bus_breaker_id' }, inplace=True)
    # we study THE P , Q ,I FROM THE GREEK BOUNDARY NODES
    filtered = X_nodes[X_nodes['bus_breaker_id'].str.startswith(('G')) ].copy() 
    filtered.columns = filtered.columns.str.strip()
    filtered['Timestamp'] = hour 
    #Give prefix to current and numeric form
    filtered = adjust_prefixes(filtered) 
    filtered = convert_to_numeric(filtered)
    return filtered

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

def convert_to_numeric(df):
    df['P'] = pd.to_numeric(df['P'], errors='coerce')
    df['Q'] = pd.to_numeric(df['Q'], errors='coerce')
    df['I'] = pd.to_numeric(df['I'], errors='coerce')
    df['Timestamp'] = pd.to_numeric(df['Timestamp'], errors='coerce')
    return df

def save_combined_data(combined, output_file):
    """
    Save the combined DataFrame to an Excel file.
    """
    #Ensure timestamp is 4 digit
    combined['Timestamp'] = combined['Timestamp'].apply(lambda x: f'{int(x):04d}')
    #Sort data and save to excel
    combined.sort_values(by=['bus_breaker_id', 'Timestamp'], inplace=True)
    combined.to_excel(output_file, index=False)       
   
def generate_plots(hours, output_file , output_folder):
    """
    Generate and save plots for current, active power, and reactive power for each bus breaker ID.
    """
    #Read the Greek X-nodes
    try:
       combine = pd.read_excel(output_file, dtype={'Timestamp': str})
    except FileNotFoundError:
        print(f"Error: {output_file} not found.")
        exit(1)
    # Ensure that all timestamps have a consistent 4-character format (e.g., '0030', '1130', etc.)
    combine['Timestamp'] = combine['Timestamp'].str.zfill(4)
    # Create a mapping of time points to numeric indices (0 for '0030', 1 for '0130', etc.)
    time_point_to_index = {time: idx for idx, time in enumerate(hours)}
    # Convert the 'Timestamp' column to the corresponding numeric index using the mapping
    combine['Timestamp_index'] = combine['Timestamp'].map(time_point_to_index)
    
    # Get unique bus breaker IDs
    bus_breaker_ids = combine['bus_breaker_id'].unique()

    # Check if there are any unmapped timestamps
    if combine['Timestamp_index'].isna().any():
        print("Warning: Some timestamps could not be mapped. Please check your data.")
        print(combine[combine['Timestamp_index'].isna()]['Timestamp'].unique())

    #Iterate through each bus_breaker_id to create the plots
    for bus_breaker_id in bus_breaker_ids:
        filtered_df = combine[combine['bus_breaker_id'] == bus_breaker_id]
        boundary_line_id = filtered_df['id'].iloc[0]  # Assume each bus_breaker_id has a single ID for titling the plots
        ticks = 15 

        # Pass boundary_line_id into plot_data for file naming
        plot_data(filtered_df, combine,'I', bus_breaker_id, 'Current (A)', f'Current Plot for {bus_breaker_id}',
                  output_folder, hours, time_point_to_index, f'{boundary_line_id}_current_plot', ticks)

        plot_data(filtered_df, combine, 'P', bus_breaker_id, 'Active Power (MW)', f'Active Power Plot for {bus_breaker_id}',
                  output_folder, hours, time_point_to_index, f'{boundary_line_id}_active_power_plot' , ticks)

        plot_data(filtered_df, combine ,'Q', bus_breaker_id, 'Reactive Power (MVAr)', f'Reactive Power Plot for {bus_breaker_id}',
                  output_folder, hours, time_point_to_index, f'{boundary_line_id}_reactive_power_plot' , ticks)

def plot_data(filtered_df, combine, column, bus_breaker_id, ylabel, title, output_folder, hours, time_point_to_index, file_suffix , ticks):
    """
    Plot data (Current, Active Power, or Reactive Power) and save the plot.
    """
    plt.figure(figsize=(10, 6)) # figure with specified size
    
    if not filtered_df['Timestamp_index'].isna().all():
        # Get the maximum and minimum values of the column (e.g., I, P, Q) and round to the nearest hundred
        max_val = math.ceil(combine[column].max() / 100) * 100
        min_val = math.floor(combine[column].min() / 100) * 100
        #Range of values
        value_range = max_val - min_val 
        # Determine the step size for y-axis ticks based on the value range
        step = calculate_step_size(value_range, ticks)
        # Calculate the limits for the y-axis based on the step size
        max_limit, min_limit = calculate_limits(max_val, min_val, step)

        # Plot the values using a scatter plot with black dots
        plt.scatter(filtered_df['Timestamp_index'], filtered_df[column], color='black', s=50, edgecolor='black', zorder=5)
        # Plot title and axis labels
        plt.title(title)
        plt.xlabel('Timestamp')
        plt.ylabel(ylabel)
        # Define the x-axis ticks as the mapped time points and rotate the labels for better visibility
        plt.xticks(ticks=list(time_point_to_index.values()), labels=hours, rotation=90)
        # Set the y-axis limits and add ticks based on the calculated step size
        plt.ylim(min_limit, max_limit)
        plt.yticks(range(min_limit, max_limit+1, step))
        # Add grid lines behind the plot and a horizontal line at y=0 for reference
        plt.grid(True, zorder=0)
        plt.axhline(y=0, color='black', linestyle='--', linewidth=1)
        # Automatically adjust the layout to prevent overlapping elements
        plt.tight_layout()
        # Save the plot to the specified folder with the corresponding bus breaker ID and file suffix
        plot_path = os.path.join(output_folder, f'{bus_breaker_id}_{file_suffix}.png')
        plt.savefig(plot_path)
        plt.close()

def calculate_step_size(range, ticks):
    """
    Calculate the step size for the plot's y-axis based on the range.
    """
    if ticks <= 0:
        raise ValueError("Number of ticks should be greater than zero.")
    
    # Divide the range by the total number of ticks to get the raw step size
    raw_step = range / (ticks * 100)
    
    # Determine the decimal portion of the step size to decide between rounding up or downn
    decimal_part = raw_step - math.floor(raw_step)
    
    # If more than 50% of the next step, round up, otherwise round down
    if decimal_part > 0.50:
        step = math.ceil(raw_step) * 100
    else:
        step = math.floor(raw_step) * 100
    # define a minimum step size if necessary
    if step == 0 :
        step = 100 
        
    return step

def calculate_limits(max, min, step):
    """
    Calculate the max and min limits for the plot's y-axis.
    """
    # Round up the maximum value to the nearest step size for the upper limit
    max_limit = step * math.ceil(max / step)
    # Round down the minimum value to the nearest step size for the lower limit
    min_limit = step * math.floor(min / step)
    return max_limit, min_limit
     
if __name__ == "__main__":
    main()
