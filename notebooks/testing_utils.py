import pandas as pd
import os
import json
import requests
from bs4 import BeautifulSoup


###### READ MEASURES FILES ######
def read_json_files_in_folder(folder_path):
    # Define a list of permissible fields for testing_as
    permissible_testing_as_fields = ["numerator_bnf_codes_filter"]
    results = []
    
    # Iterate over all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            filename_without_extension = os.path.splitext(filename)[0]
            
            # Read the JSON file
            with open(file_path, 'r') as file:
                data = json.load(file)
                
                # Check if 'testing_measure' exists and is True
                if data.get('testing_measure') is True:
                    try:
                        # Check if 'testing_type' is not None
                        testing_type = data.get('testing_type')
                        if testing_type is None:
                            raise ValueError(f"In the file {filename_without_extension}, 'testing_type' is not defined.")
                        
                        # Ensure 'testing_as' is one of the permissible fields or 'custom'
                        if testing_type != 'custom' and testing_type not in permissible_testing_as_fields:
                            raise ValueError(f"In the file {filename_without_extension}, 'testing_type' must be one of {permissible_testing_as_fields} or 'custom'.")
                    
                        # Prepare the result dictionary
                        result = {
                            'filename': filename_without_extension,
                            'testing_measure': data.get('testing_measure'),
                            'testing_comments': data.get('testing_comments'),
                            'testing_type': testing_type
                        }
                    
                        # Get data to test against if 'testing_type' is not 'custom'
                        if testing_type != 'custom':
                            result['testing_type_data'] = data.get(testing_type)
                            if result['testing_type_data'] is None:
                                raise ValueError(f"In the file {filename_without_extension}, data for '{testing_type}' is missing or invalid.")
                        elif testing_type == 'custom':
                            # Handle custom case with include/exclude logic
                            result['testing_include'] = data.get('testing_include')
                            result['testing_exclude'] = data.get('testing_exclude')
                    
                            if result['testing_include'] is None or result['testing_exclude'] is None:
                                raise ValueError(f"In the file {filename_without_extension}, both 'testing_include' and 'testing_exclude' must be provided when 'testing_type' is 'custom'.")
                    
                        # Append the result to the results list
                        results.append(result)
                    
                    except ValueError as e:
                        # Handle the error or log it accordingly
                        print(f"Error: {e}")
    return results


# GitHub URL to scrape the list of JSON files
base_url = "https://github.com/ebmdatalab/openprescribing/tree/main/openprescribing/measures/definitions"
raw_base_url = "https://raw.githubusercontent.com/ebmdatalab/openprescribing/main/openprescribing/measures/definitions/"

def get_json_files_from_github(url):
    # Send a request to get the HTML content
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to load page {url}")
    
    # Parse the page with BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find all JSON file links on the page
    json_files = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.json'):  # Only select .json files
            file_name = href.split('/')[-1]
            json_files.append(file_name)
    
    return json_files

def load_json_file(file_name):
    # Construct the raw GitHub URL for the JSON file
    file_url = raw_base_url + file_name
    response = requests.get(file_url)
    
    if response.status_code == 200:
        return response.json()  # Return JSON content as a Python dict
    else:
        raise Exception(f"Failed to load JSON file {file_name}")
    
def read_json_files_in_github():
    # Define a list of permissible fields for testing_as
    permissible_testing_as_fields = ["numerator_bnf_codes_filter"]
    results = []

    # Step 1: Get list of JSON files from the GitHub directory
    json_files = get_json_files_from_github(base_url)

    # Step 2: Load each JSON file and process it
    for file_name in json_files:
        try:
            filename_without_extension = os.path.splitext(file_name)[0]
            
            # Read the JSON data using the load_json_file function
            json_data = load_json_file(file_name)

            # Check if 'testing_measure' exists and is True
            if json_data.get('testing_measure') is True:
                try:
                    # Check if 'testing_type' is not None
                    testing_type = json_data.get('testing_type')
                    if testing_type is None:
                        raise ValueError(f"In the file {filename_without_extension}, 'testing_type' is not defined.")
                    
                    # Ensure 'testing_as' is one of the permissible fields or 'custom'
                    if testing_type != 'custom' and testing_type not in permissible_testing_as_fields:
                        raise ValueError(f"In the file {filename_without_extension}, 'testing_type' must be one of {permissible_testing_as_fields} or 'custom'.")
                
                    # Prepare the result dictionary
                    result = {
                        'filename': filename_without_extension,
                        'testing_measure': json_data.get('testing_measure'),
                        'testing_comments': json_data.get('testing_comments'),
                        'testing_type': testing_type
                    }
                
                    # Get data to test against if 'testing_type' is not 'custom'
                    if testing_type != 'custom':
                        result['testing_type_data'] = json_data.get(testing_type)
                        if result['testing_type_data'] is None:
                            raise ValueError(f"In the file {filename_without_extension}, data for '{testing_type}' is missing or invalid.")
                    elif testing_type == 'custom':
                        # Handle custom case with include/exclude logic
                        result['testing_include'] = json_data.get('testing_include')
                        result['testing_exclude'] = json_data.get('testing_exclude')
                
                        if result['testing_include'] is None or result['testing_exclude'] is None:
                            raise ValueError(f"In the file {filename_without_extension}, both 'testing_include' and 'testing_exclude' must be provided when 'testing_type' is 'custom'.")
                
                    # Append the result to the results list
                    results.append(result)
                
                except ValueError as e:
                    # Handle specific ValueError
                    print(f"Error in file {filename_without_extension}: {e}")
        
        except Exception as e:
            # Catch all other exceptions like network issues or parsing problems
            print(f"Failed to process {file_name}: {e}")
    
    return results

#####################################################################################################

# Convert wildcard patterns to regex patterns
def wildcard_to_regex(pattern):
    return pattern.replace('%', '.*')

# Filter the DataFrame based on include and exclude lists
def filter_include_exclude_dataframe(df, testing_include, testing_exclude):
    # Create a boolean mask for include patterns
    include_mask = pd.Series(False, index=df.index)
    for pattern in testing_include:
        regex_pattern = wildcard_to_regex(pattern)
        include_mask |= df['BNF_CODE'].str.contains(regex_pattern)

    # Create a boolean mask for exclude patterns
    exclude_mask = pd.Series(False, index=df.index)
    for pattern in testing_exclude:
        regex_pattern = wildcard_to_regex(pattern)
        exclude_mask |= df['BNF_CODE'].str.contains(regex_pattern)

    # Filter DataFrame: include and not exclude
    filtered_df = df[include_mask & ~exclude_mask]
    return filtered_df    

def filter_num_bnf_codes_dataframe(df, testing_data):
    # Using list comprehension to remove everything from ' # ' onwards
    cleaned_data = [item.split(' # ')[0] for item in testing_data]

    # Append '.*' to each item in the cleaned_data list
    cleaned_data = [item + '%' for item in cleaned_data]

    # Creating the include list by including items that don't start with '~'
    include_list = [item for item in cleaned_data if not item.startswith('~')]
    
    # Creating the exclude list by including items starting with '~' and removing the '~'
    exclude_list = [item[1:] for item in cleaned_data if item.startswith('~')]

    # Create a boolean mask for include patterns
    include_mask = pd.Series(False, index=df.index)
    for pattern in include_list:
        regex_pattern = wildcard_to_regex(pattern)
        include_mask |= df['BNF_CODE'].str.contains(regex_pattern)

    # Create a boolean mask for exclude patterns
    exclude_mask = pd.Series(False, index=df.index)
    for pattern in exclude_list:
        regex_pattern = wildcard_to_regex(pattern)
        exclude_mask |= df['BNF_CODE'].str.contains(regex_pattern)

    # Filter DataFrame: include and not exclude
    filtered_df = df[include_mask & ~exclude_mask]
    return filtered_df    

def measures_filter(df, measure_data):
    if (measure_data['testing_type'] == 'custom'):
        filtered_df = filter_include_exclude_dataframe(df, measure_data['testing_include'], measure_data['testing_exclude'])
    elif (measure_data['testing_type'] == "numerator_bnf_codes_filter"):
        filtered_df = filter_num_bnf_codes_dataframe(df, measure_data['testing_type_data'])
    else:
        print (f"Unknown testing type {measure_data['testing_type']}")

    result = {
        "title": f"{measure_data['filename']}.json",
        "comments": measure_data['testing_comments'],
        "data": filtered_df
    }

    if not filtered_df.empty:
        result["test_triggered"] = True
    else:
        result["test_triggered"] = False
    return result
    
####### HTML REPORT CREATION #######

def write_monthly_testing_report_html(triggered_tests, passed_tests, date):
    reports_dir = os.path.join("..", "reports")
    os.makedirs(reports_dir, exist_ok=True)

    # Create an alert if January data to explain BNF structure changes
    jan_alert = ''
    if date[-2:] == '01':
        jan_alert = (
            f'<p><b>Please note:</b> January data often includes a larger number of "changes" as BNF structure changes are generally made in January data - '
            f'<a href="https://www.nhsbsa.nhs.uk/bnf-code-changes-january-{date[:4]}">more information here</a></p>'
        )

    # Read the base64 image string from the file
    with open("base64_image.txt", "r") as file:
        base64_image = file.read()

    tick_svg = '<svg id="Layer_1" data-name="Layer 1" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 122.88 101.6"><defs><style>.cls-1{fill:#15b01a;}</style></defs><title>tick-green</title><path class="cls-1" d="M4.67,67.27c-14.45-15.53,7.77-38.7,23.81-24C34.13,48.4,42.32,55.9,48,61L93.69,5.3c15.33-15.86,39.53,7.42,24.4,23.36L61.14,96.29a17,17,0,0,1-12.31,5.31h-.2a16.24,16.24,0,0,1-11-4.26c-9.49-8.8-23.09-21.71-32.91-30v0Z"/></svg>'
    cross_svg = '<?xml version="1.0" encoding="utf-8"?><svg version="1.1" id="Layer_1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" x="0px" y="0px" viewBox="0 0 91.06 122.88" style="enable-background:new 0 0 91.06 122.88" xml:space="preserve"><g><path d="M58.68,84.96H27.37v-3.12c0-5.32,0.59-9.65,1.8-12.97c1.21-3.35,3.01-6.36,5.4-9.11c2.39-2.76,7.76-7.6,16.12-14.52c4.45-3.63,6.67-6.95,6.67-9.96c0-3.04-0.9-5.37-2.67-7.06c-1.8-1.66-4.5-2.5-8.13-2.5c-3.91,0-7.12,1.29-9.68,3.88c-2.56,2.56-4.19,7.09-4.9,13.5L0,39.13c1.1-11.76,5.37-21.21,12.8-28.39C20.25,3.57,31.68,0,47.06,0c11.98,0,21.63,2.5,29,7.48c9.99,6.78,15,15.78,15,27.04c0,4.67-1.29,9.2-3.88,13.53c-2.56,4.33-7.85,9.65-15.81,15.89c-5.54,4.42-9.06,7.93-10.52,10.61C59.42,77.19,58.68,80.68,58.68,84.96L58.68,84.96z M26.28,93.29h33.56v29.6H26.28V93.29L26.28,93.29z" fill="#f97306"/></g></svg>'
    # Start the HTML report
    report = f"""
    <html>
    <head>
    <title>Monthly Testing Report for {date}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f8f9fa;
            margin: 20px;
            color: #333;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
        }}
        header {{
            text-align: center;
            margin-bottom: 40px;
        }}
        header img {{
            max-width: 650px;
            margin-bottom: 10px;
        }}
        h2 {{
            color: #333;
        }}
        h3 {{
            color: #333;
            margin-top: 30px;
        }}
        p {{
            margin-bottom: 15px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            margin-bottom: 20px;
        }}
        table, th, td {{
            border: 1px solid #333;
        }}
        th {{
            background-color: #0485d1;
            color: white;
            padding: 10px;
            text-align: left;
        }}
        td {{
            padding: 8px;
            text-align: left;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        a {{
            text-decoration: none;
            color: #0485d1;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
    </head>
    <body>
    <div class="container">
        <header>
            <img src="{base64_image}" alt="OpenPrescribing logo">
            <h2>Monthly Testing Report for {date}</h2>
        </header>
        <p>This report details testing results for OpenPrescribing measures which have the flag testing_measure = true. Items appearing in the English Prescribing Data for {date} that have not previously appeared in the data (from Jan 2014).</p>
        {jan_alert}
        <p><a href="https://html-preview.github.io/?url=https://github.com/chrisjwood16/openprescribing_tests/blob/main/reports/list_testing_reports.html">View previous reports</a></p>
    """

    # Check if there are any triggered tests
    if len(triggered_tests) == 0:
        report += "<h3>All tests passed</h3>"
    else:
        report += "<h2>Tests returning results:</h2>"
        for item in triggered_tests:
            report += f"<a href='https://github.com/ebmdatalab/openprescribing/tree/main/openprescribing/measures/definitions/{item['title']}'><h3>{item['title']} {cross_svg}</h3></a>"
            report += f"<p>{item['comments']}</p>"
            df = item['data'][["BNF_CODE", "BNF_DESCRIPTION", "CHEMICAL_SUBSTANCE_BNF_DESCR"]]
            report += f"<p>{df.to_html(index=False, classes='table')}</p>"
        report += "<h2>Tests passed:</h2>"
        if len(passed_tests) == 0:
            report += "<p>No passed tests</p>"
        else:
            for item in passed_tests:
                report += f"<p><a href='https://github.com/ebmdatalab/openprescribing/tree/main/openprescribing/measures/definitions/{item['title']}'>{item['title']}</a>  {tick_svg}<span style='color: green;'>&#10004;</span></p>"

    report += """
    </div>
    </body>
    </html>
    """

    # Write the report to a file
    with open(f"{reports_dir}/monthly_test_report_{date}.html", "w") as file:
        file.write(report)

    print(f"Report written to {reports_dir}/monthly_testing_report_{date}.html")



def generate_list_reports_html():
    reports_dir = os.path.join("..", "reports")
    
    # Read the base64 image string from the file
    with open("base64_image.txt", "r") as file:
        base64_image = file.read()

    # Get all HTML files in the directory, except list_reports.html
    html_files = [f for f in os.listdir(reports_dir) if f.endswith('.html') and f != 'list_reports.html' and f != 'list_test_reports.html' and f.startswith('monthly_test_report')]

    # Start the HTML content with the Base64 logo embedded in the header
    html_content = f"""
    <html>
    <head>
    <title>English Prescribing Data - Monthly Test Reports</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f8f9fa;
            margin: 20px;
        }}
        a {{
            text-decoration: none;
            color: #0485d1;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        h2 {{
            color: #333;
        }}
        li {{
            margin: 10px 0;
        }}
        header {{
            text-align: center;
            margin-bottom: 40px;
        }}
        header img {{
            max-width: 650px;
            margin-bottom: 10px;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
        }}
    </style>
    </head>
    <body>
    <div class="container">
        <header>
            <img src="{base64_image}" alt="OpenPrescribing logo">
            <h2>English Prescribing Data - Monthly Test Reports</h2>
        </header>
        <ul>
    """

    # Add links to all HTML files
    for html_file in html_files:
        title = os.path.splitext(html_file)[0]
        # Create title for month and year
        title = title.split('_')[-1]
        title = pd.to_datetime(title).strftime('%B %Y')
        link = f"https://html-preview.github.io/?url=https://github.com/chrisjwood16/openprescribing_tests/blob/main/reports/{html_file}"
        html_content += f'<li><a href="{link}">{title}</a></li>\n'

    # End the HTML content
    html_content += """
    </ul>
    </div>
    </body>
    </html>
    """

    # Write the HTML content to list_reports.html
    with open(os.path.join(reports_dir, 'list_test_reports.html'), 'w') as f:
        f.write(html_content)

def run_tests(bnf_codes_df, date_for):
    folder_path = '../measures_to_test'  # Temporary line to test locally
    measures_json = read_json_files_in_folder(folder_path) # Temporary line to test locally

    #measures_json = read_json_files_in_github() # Uncomment this line to use GitHub files after testing locally

    # Load the CSV file into a pandas DataFrame - Remove this line to use passed variable version after testing
    bnf_codes_df = pd.read_csv('new_bnf_codes.csv') # Temporary line to test locally - Remove this line after testing - will use passed variable version

    # Create an empty list for triggered tests
    triggered_tests = []
    passed_tests = []

    for measure_data in measures_json:
        test_result = measures_filter(bnf_codes_df, measure_data)
        if (test_result["test_triggered"]):
            triggered_tests.append(test_result)
        else:
            passed_tests.append(test_result)

    write_monthly_testing_report_html(triggered_tests, passed_tests, date_for)
    generate_list_reports_html()