### Retail ETL Pipeline and Data Exploration Project
This project aims to construct an ETL pipeline that extracts, transforms, and loads retail data from a CSV file into a database, along with basic data exploration.

### Environment Setup 
First clone respository 

    git clone https://github.com/yourusername/your-repository.git
    cd your-repository

Second set up your virtual environment to avoid conflicts with other python projects 

Example using conda with python version that you need:

    # Navigate to your project root directory
    cd path_to_your_project_root

    # Create a new Conda environment named `retail_etl`
    conda create -n retail_etl python=3.12.4 

    # Activate the environment
    conda activate retail_etl
    
Third install project dependecies

    pip install -r requirements.txt

### Project Structure
    project_root/
    ├── data_exploration/
    │   └── data_explore.py
    ├── db/
    │   ├── retail_15_01_2022.csv
    │   └── retail.db
    ├── etl_workflow/
    │   ├── base_etl.py
    │   ├── config.py
    │   ├── etl_pipeline.py
    │   ├── extract.py
    │   ├── load.py
    │   └── transform.py
    └── tests/
        ├── __init__.py
        ├── common_test_utilities.py   
        ├── test_base_etl.py   
        ├── test_data_15_01_2015.py
        ├── test_database.db
        ├── test_etl_pipeline.py
        ├── test_extract.py
        ├── test_load.py
        └── test_transform.py

### Launch Project
To run the ETL pipeline and perform data exploration, navigate to the project root directory and type:
    
    python data_exploration/data_explore.py

Make sure you are in the root directory, or else modify your PYTHONPATH accordingly.

### Configuration
The configuration file etl_workflow/config.py allows customization of:

CSV File Path: Specify the path to your retail data CSV.
Database Connection: Set up your database credentials.
Batch Size: Define the number of records to process at once.
Data Integration Method: Choose between ignoring duplicates (ignore) or updating existing records (update)

Example configuration setup with default options:

    config = {
        "csv_file": "db/my_csv_file_15_01_2022.csv",
        "database": "db/retail.db",
        "batch_size": 0,
        "integration_method": "ignore"  # Option: "update"
    }

### Naming Convention for CSV Files
Ensure your CSV files follow the naming convention:

    my_csv_file_dd_mm_yyyy.csv

Example: my_csv_file_15_01_2022.csv

### ETL Pipeline
Each ETL process (extract, transform, load) is modularized for scalability and ease of testing. The pipeline is orchestrated in etl_pipeline.py.

### Running Tests
Unit tests for the ETL pipeline are provided. Run all tests using:

    python -m unittest discover -s tests

Or test individual components:

    python -m unittest tests/test_base_etl.py

Tests use a dedicated test database and CSV files.

### Troubleshooting
ModuleNotFoundError: Ensure you are running scripts from the project root and your PYTHONPATH is correctly set.

Database Connection Issues: Verify your database credentials in config.py.
