import requests
import random  # For generating a random integer

def execute_sql_query(sql_query, client_id):
    # Superset API URL and credentials
    SUPSERSET_BASE_URL = "http://localhost:8088/"
    LOGIN_ENDPOINT = SUPSERSET_BASE_URL + "api/v1/security/login"
    CSRF_TOKEN_ENDPOINT = SUPSERSET_BASE_URL + "api/v1/security/csrf_token/"
    SQL_EXECUTE_ENDPOINT = SUPSERSET_BASE_URL + "api/v1/sqllab/execute"
    SQL_EXPORT_ENDPOINT_TEMPLATE = SUPSERSET_BASE_URL + "api/v1/sqllab/export/{client_id}/"

    login_payload = {
        "username": "admin",
        "password": "admin",
        "provider": "db",
        "refresh": True
    }

    # Session to persist cookies and headers
    session = requests.Session()

    try:
        # Login to get access token
        response = session.post(LOGIN_ENDPOINT, json=login_payload)
        if response.status_code != 200:
            return f"Login Failed. Status Code: {response.status_code}"

        access_token = response.json()["access_token"]

        # Prepare headers for CSRF token request
        csrf_headers = {"Authorization": "Bearer " + access_token}

        # Fetch CSRF token with Authorization header
        csrf_response = session.get(CSRF_TOKEN_ENDPOINT, headers=csrf_headers)
        if csrf_response.status_code != 200:
            return f"Failed to get CSRF Token. Status Code: {csrf_response.status_code}"

        csrf_token = csrf_response.json()["result"]

        # Headers for subsequent requests
        headers = {
            "Authorization": "Bearer " + access_token,
            "X-CSRFToken": csrf_token
        }

        # Payload for SQL execution
        sql_execute_payload = {
            "client_id": str(client_id),
            "database_id": 1,
            "expand_data": True,
            "json": True,
            "queryLimit": 100,
            "runAsync": False,
            "schema": "public",
            "sql": sql_query,
        }

        # Execute SQL query
        data_response = session.post(SQL_EXECUTE_ENDPOINT, json=sql_execute_payload, headers=headers)
        if data_response.status_code != 200:
            error_message = data_response.json().get('message', 'No specific error message provided')
            return f"SQL Execution Failed. Status Code: {data_response.status_code}. Error: {error_message}" 

        export_url = SQL_EXPORT_ENDPOINT_TEMPLATE.format(client_id=client_id)
        export_response = session.get(export_url, headers=headers)
        if export_response.status_code != 200:
            return f"Export Failed. Status Code: {export_response.status_code}"

        # Save the CSV file
        csv_filename = f"query_results_{client_id}.csv"
        with open(csv_filename, 'wb') as file:
            file.write(export_response.content)

        return f"Query results exported to {csv_filename}"

    except requests.exceptions.RequestException as e:
        return f"An error occurred: {e}"

# Example usage of the function
query = "SELECT * FROM public.distribution_points;"
client_id = random.randint(1000, 9999)  # Generate a random integer

print("client_id",client_id)

query_result = execute_sql_query(query, client_id)
print(query_result)
