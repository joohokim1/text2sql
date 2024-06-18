# text2sql


## Prerequisites

 * Google BigQuery Service Account JSON file
 * Anthropic API Key

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/joohokim1/text2sql.git
    ```
2. Navigate to the project directory:
    ```sh
    cd text2sql
    ```
3. Save the Google BigQuery Service Account JSON file in the project directory as `bigquery_key.json`.
4. Save the Anthropic API Key string as a file named `anthropic_key.txt` in the project directory. 
5. Create virtual environments:
    ```sh
    python -m venv myvenv
    ```
6. Use virtual environments:
    ```sh
    source myvenv/bin/activate
    ```
7. Install requirements:
    ```sh
    (myvenv) pip install -r requirements.txt
    ```
8. Start the application:
    ```sh
    (myvenv) streamlit run main.py
    ```
