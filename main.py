from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import mysql.connector
import io
import logging
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Database connection function
def get_db_connection():
    logger.info("Establishing database connection...")
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Define the mapping between Excel names and database names
name_mapping = {
    'ASSESSMENT - Financial Health': 'Financial Health',
    'ASSESSMENT - Business Competency': 'Business Competency',
    'ASSESSMENT - Leadership & Team Capabilities': 'Leadership & Team Capabilities',
    'ASSESSMENT - Personality & Change Readiness': 'Personality & Change Readiness',
    'ASSESSMENT - Technical Competency-Agribusiness': 'Technical Competency-Agribusiness'
}

# Route for uploading Excel and updating existing rows in the MySQL table
@app.post("/insert_assessment_score_2/")
async def upload_excel(file: UploadFile = File(...)):
    logger.info("Received file upload...")
    # Read the uploaded Excel file into pandas DataFrame, using "Agribusiness" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Agribusiness")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # SQL query for updating scores
    update_query = "UPDATE tbl_assessmentscore SET ascore_score = %s WHERE ascore_name = %s AND ascore_assessmentId = 2"

    try:
        logger.info("Executing updates...")
        # Iterate through each row in the DataFrame
        for index, row in excel_data.iterrows():
            logger.info(f"Updating scores for row {index + 1}...")
            
            # For each assessment name, update the corresponding score
            for excel_name, db_name in name_mapping.items():
                if excel_name in row:
                    score = row[excel_name]  # Get the score for this specific row
                    logger.info(f"Updating {db_name} with score {score} for row {index + 1}")
                    cursor.execute(update_query, (float(score), db_name))  # Update the database
                else:
                    logger.warning(f"{excel_name} not found in Excel columns.")
        
        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All scores updated successfully.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
    finally:
        cursor.close()
        connection.close()

    return {"message": "Scores updated successfully for all rows in Excel."}

# Route for uploading Excel and updating existing rows in the MySQL table for assessment ID 1
@app.post("/insert_assessment_score_1/")
async def upload_excel_1(file: UploadFile = File(...)):
    logger.info("Received file upload for assessment ID 1...")
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Manufacturing")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # SQL query for updating scores
    update_query = "UPDATE tbl_assessmentscore SET ascore_score = %s WHERE ascore_name = %s AND ascore_assessmentId = 1"

    try:
        logger.info("Executing updates for assessment ID 1...")
        # Iterate through each row in the DataFrame
        for index, row in excel_data.iterrows():
            logger.info(f"Updating scores for row {index + 1}...")
            
            # For each assessment name, update the corresponding score
            for excel_name, db_name in name_mapping.items():
                if excel_name in row:
                    score = row[excel_name]  # Get the score for this specific row

                    # Check for NaN values and skip if found
                    if pd.isna(score):
                        logger.warning(f"Score is NaN for {db_name} in row {index + 1}. Skipping update.")
                        continue  # Skip this score if it's NaN

                    logger.info(f"Updating {db_name} with score {score} for row {index + 1}")
                    cursor.execute(update_query, (float(score), db_name))  # Update the database
        
        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All scores for assessment ID 1 updated successfully.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
    finally:
        cursor.close()
        connection.close()

    return {"message": "Scores updated successfully for all rows in Excel for assessment ID 1."}

# Route for uploading Excel and updating existing names in the tbl_entrepreneur table
@app.post("/insert_entrepreneur_names_2/")
async def update_names(file: UploadFile = File(...)):
    logger.info("Received file upload...")
    # Read the uploaded Excel file into pandas DataFrame, using "Agribusiness" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Agribusiness")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch the rows where ep_sector = 47
    fetch_query = "SELECT ep_id FROM tbl_entrepreneur WHERE ep_sector = 47"
    cursor.execute(fetch_query)
    rows = cursor.fetchall()

    if len(rows) < 22:
        logger.error("Not enough rows with ep_sector = 47 to update.")
        return {"error": "Not enough rows to update."}

    try:
        logger.info("Updating names from the first 22 rows where ep_sector = 47...")
        # Iterate through the first 22 records in the Excel sheet and update names
        for index, row in excel_data.iterrows():
            if index >= 22:
                break  # Only take the first 22 names from the Excel file

            ep_name = row['Name']  # Column in Excel with names

            ep_id = rows[index][0]  # Get the ep_id of the corresponding row in the database
            logger.info(f"Updating ep_name '{ep_name}' for ep_id = {ep_id}")

            # Update query for the name
            update_query = """
                UPDATE tbl_entrepreneur 
                SET ep_name = %s 
                WHERE ep_id = %s AND ep_sector = 47
            """
            cursor.execute(update_query, (ep_name, ep_id))

        # Commit the updates after processing all 22 rows
        connection.commit()
        logger.info("All names updated successfully.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
    finally:
        cursor.close()
        connection.close()

    return {"message": "Names updated successfully for all 22 rows where ep_sector = 47."}

@app.post("/insert_entrepreneur_names_1/")
async def update_names_sector_46(file: UploadFile = File(...)):
    logger.info("Received file upload for sector 46...")

    # Read the uploaded Excel file into pandas DataFrame, using "Manufacturing" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Manufacturing")

    # Log the shape of the DataFrame
    logger.info(f"Excel data shape: {excel_data.shape}")

    # Check that the number of rows in the Excel file is correct
    if len(excel_data) != 114:
        logger.error(f"Expected 114 rows in the Excel file, but found {len(excel_data)}.")
        return {"error": "Excel file does not contain 114 rows."}

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch the IDs of the existing rows where ep_sector = 46
    cursor.execute("SELECT ep_id FROM tbl_entrepreneur WHERE ep_sector = 46")
    existing_ids = cursor.fetchall()  # Fetch all IDs for sector 46

    # Check if we fetched the expected number of IDs
    if len(existing_ids) != 114:
        logger.error(f"Expected 114 IDs for sector 46, but got {len(existing_ids)}. Existing IDs: {existing_ids}")
        return {"error": "Mismatch between Excel rows and database rows."}

    # SQL query for updating names
    update_query = "UPDATE tbl_entrepreneur SET ep_name = %s WHERE ep_id = %s"

    try:
        logger.info("Updating names for sector 46...")
        # Iterate through each row in the DataFrame and update names
        for index, row in excel_data.iterrows():
            ep_id = existing_ids[index][0]  # Get the corresponding ep_id
            ep_name = row['Name']  # Get the name from the DataFrame

            logger.info(f"Updating name '{ep_name}' for ep_id {ep_id}...")
            cursor.execute(update_query, (ep_name, ep_id))  # Update the database

        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All names updated successfully for sector 46.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
    finally:
        cursor.close()
        connection.close()

    return {"message": "Names updated successfully for sector 46."}

@app.post("/insert_entrepreneur_emails_2/")
async def update_emails(file: UploadFile = File(...)):
    logger.info("Received file upload for emails...")
    
    # Read the uploaded Excel file into pandas DataFrame, using "Agribusiness" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Agribusiness")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch the rows where ep_sector = 47
    fetch_query = "SELECT ep_id FROM tbl_entrepreneur WHERE ep_sector = 47"
    cursor.execute(fetch_query)
    rows = cursor.fetchall()

    # Check if there are enough rows with ep_sector = 47
    if len(rows) < 22:
        logger.error("Not enough rows with ep_sector = 47 to update.")
        return {"error": "Not enough rows to update."}

    try:
        logger.info("Updating emails for the first 22 rows where ep_sector = 47...")
        # Iterate through the first 22 records in the Excel sheet and update emails
        for index, row in excel_data.iterrows():
            if index >= 22:
                break  # Only take the first 22 emails from the Excel file

            ep_email = row['Email']  # Assuming the email column is named 'Email'
            ep_id = rows[index][0]  # Get the ep_id of the corresponding row in the database

            logger.info(f"Updating ep_email '{ep_email}' for ep_id = {ep_id}")

            # Update query for the email
            update_query = """
                UPDATE tbl_entrepreneur 
                SET ep_email = %s 
                WHERE ep_id = %s AND ep_sector = 47
            """
            cursor.execute(update_query, (ep_email, ep_id))

        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All emails updated successfully.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
    finally:
        cursor.close()
        connection.close()

    return {"message": "Emails updated successfully for the first 22 rows where ep_sector = 47."}

@app.post("/insert_entrepreneur_emails_1/")
async def update_emails_sector_46(file: UploadFile = File(...)):
    logger.info("Received file upload for sector 46...")

    # Read the uploaded Excel file into pandas DataFrame, using "Manufacturing" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Manufacturing")

    # Log the shape of the DataFrame
    logger.info(f"Excel data shape: {excel_data.shape}")

    # Check that the number of rows in the Excel file is correct
    if len(excel_data) != 114:
        logger.error(f"Expected 114 rows in the Excel file, but found {len(excel_data)}.")
        return {"error": "Excel file does not contain 114 rows."}

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch the IDs of the existing rows where ep_sector = 46
    cursor.execute("SELECT ep_id FROM tbl_entrepreneur WHERE ep_sector = 46")
    existing_ids = cursor.fetchall()  # Fetch all IDs for sector 46

    # Check if we fetched the expected number of IDs
    if len(existing_ids) != 114:
        logger.error(f"Expected 114 IDs for sector 46, but got {len(existing_ids)}. Existing IDs: {existing_ids}")
        return {"error": "Mismatch between Excel rows and database rows."}

    # SQL query for updating emails
    update_query = "UPDATE tbl_entrepreneur SET ep_email = %s WHERE ep_id = %s"

    try:
        logger.info("Updating emails for sector 46...")
        # Iterate through each row in the DataFrame and update emails
        for index, row in excel_data.iterrows():
            ep_id = existing_ids[index][0]  # Get the corresponding ep_id
            ep_email = row['Email']  # Get the email from the DataFrame

            logger.info(f"Updating email '{ep_email}' for ep_id {ep_id}...")
            cursor.execute(update_query, (ep_email, ep_id))  # Update the database

        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All emails updated successfully for sector 46.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
    finally:
        cursor.close()
        connection.close()

    return {"message": "Emails updated successfully for sector 46."}

@app.post("/insert_entrepreneurlog_names_2/")
async def update_names_sector_47(file: UploadFile = File(...)):
    logger.info("Received file upload for sector 47 log...")

    # Read the uploaded Excel file into pandas DataFrame, using "Agribusiness" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Agribusiness")

    # Log the shape of the DataFrame
    logger.info(f"Excel data shape: {excel_data.shape}")

    # Check that the number of rows in the Excel file is correct
    if len(excel_data) != 22:
        logger.error(f"Expected 22 rows in the Excel file, but found {len(excel_data)}.")
        return {"error": "Excel file does not contain 22 rows."}

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch all the rows where ep_sector = 47, but only select unique eplog_epId
    cursor.execute("SELECT DISTINCT eplog_epId, eplog_id FROM tbl_entrepreneurlog WHERE eplog_sector = 47 ORDER BY eplog_epId, eplog_id")
    unique_rows = cursor.fetchall()  # Fetch unique eplog_epId with eplog_id

    # Ensure that we have exactly 22 unique rows for sector 47
    if len(unique_rows) != 22:
        logger.error(f"Expected 22 unique IDs for sector 47 log, but got {len(unique_rows)}.")
        return {"error": "Mismatch between Excel rows and unique database rows."}

    # SQL query for updating names
    update_query = "UPDATE tbl_entrepreneurlog SET eplog_name = %s WHERE eplog_id = %s"

    try:
        logger.info("Updating names for sector 47 log...")
        # Iterate through each row in the DataFrame and update names
        for index, row in excel_data.iterrows():
            eplog_name = row['Name']  # Get the name from the DataFrame
            eplog_id = unique_rows[index][1]  # Get the corresponding eplog_id

            logger.info(f"Updating name '{eplog_name}' for eplog_id {eplog_id}...")

            # Update the name for the first occurrence of each eplog_epId
            cursor.execute(update_query, (eplog_name, eplog_id))  # Update the database

        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All names updated successfully for sector 47 log.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

    return {"message": "Names updated successfully for sector 47 log."}

@app.post("/insert_entrepreneurlog_names_1/")
async def update_names_sector_46(file: UploadFile = File(...)):
    logger.info("Received file upload for sector 46 log...")

    # Read the uploaded Excel file into pandas DataFrame, using "Manufacturing" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Manufacturing")

    # Log the shape of the DataFrame
    logger.info(f"Excel data shape: {excel_data.shape}")

    # Check that the number of rows in the Excel file is correct
    if len(excel_data) != 114:
        logger.error(f"Expected 114 rows in the Excel file, but found {len(excel_data)}.")
        return {"error": "Excel file does not contain 114 rows."}

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch all the rows where ep_sector = 46, but only select unique eplog_epId
    cursor.execute("SELECT DISTINCT eplog_epId, eplog_id FROM tbl_entrepreneurlog WHERE eplog_sector = 46 ORDER BY eplog_epId, eplog_id")
    unique_rows = cursor.fetchall()  # Fetch unique eplog_epId with eplog_id

    # Ensure that we have exactly 114 unique rows for sector 46
    if len(unique_rows) != 114:
        logger.error(f"Expected 114 unique IDs for sector 46 log, but got {len(unique_rows)}.")
        return {"error": "Mismatch between Excel rows and unique database rows."}

    # SQL query for updating names
    update_query = "UPDATE tbl_entrepreneurlog SET eplog_name = %s WHERE eplog_id = %s"

    try:
        logger.info("Updating names for sector 46 log...")
        # Iterate through each row in the DataFrame and update names
        for index, row in excel_data.iterrows():
            eplog_name = row['Name']  # Get the name from the DataFrame
            eplog_id = unique_rows[index][1]  # Get the corresponding eplog_id

            logger.info(f"Updating name '{eplog_name}' for eplog_id {eplog_id}...")

            # Update the name for the first occurrence of each eplog_epId
            cursor.execute(update_query, (eplog_name, eplog_id))  # Update the database

        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All names updated successfully for sector 46 log.")
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

    return {"message": "Names updated successfully for sector 46 log."}

@app.post("/insert_trait_scores_2/")
async def upload_excel(file: UploadFile = File(...)):
    logger.info("Received file upload for trait scores...")

    # Read the uploaded Excel file into pandas DataFrame, using "Agribusiness" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Agribusiness")

    # Log the column names from the Excel file to help with debugging
    logger.info(f"Excel columns: {list(excel_data.columns)}")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # SQL query for updating scores in the scoredetails table
    update_query = """
        UPDATE tbl_scoredetails 
        SET scrd_score = %s 
        WHERE scrd_msubid = %s AND scrd_secId = 2
    """

    # Define a mapping between Excel trait names and scrd_msubid
    trait_name_mapping = {
            "TRAIT_Revenue & Profit Calculation" : 1,
            "TRAIT_Financial Management " : 2,
            "TRAIT_Capital & Expenses Assessment ": 3,
            "TRAIT_People Development" : 4,
            "TRAIT_Sales & Marketing" : 5,
            "TRAIT_ Business Operation Management " : 6,
            "TRAIT_Business Strategic Management " : 7,
            "TRAIT_Vision" : 8,
            "TRAIT_Decisiveness" : 9,
            "TRAIT_Strategic Thinking" : 10,
            "TRAIT_Collaboration": 11,
            "TRAIT_Team Management " : 12,
            "TRAIT_Conscientiousness" : 13,
            "TRAIT_Agreeableness" : 14,
            "TRAIT_Extraversion" : 15,
            "TRAIT_Emotional Stability" : 16,
            "TRAIT_Humility" : 17,
            "TRAIT_Change Readiness" : 18,
            "TRAIT_Technology & Innovation" : 33,
            "TRAIT_Quality" : 34,
            "TRAIT_Research & Development " : 35,
            "TRAIT_Planning & Execution" : 36
    }

    try:
        logger.info("Executing trait score updates...")

        # Ensure that the Excel has the correct trait columns
        missing_cols = [col for col in trait_name_mapping.keys() if col not in excel_data.columns]
        if missing_cols:
            logger.error(f"Missing columns in Excel file: {missing_cols}")
            return {"error": f"Missing columns in Excel file: {missing_cols}"}
        
        # Iterate through each row in the Excel DataFrame
        for index, row in excel_data.iterrows():
            logger.info(f"Updating trait scores for record {index + 1}...")

            # Iterate through each trait name and update the corresponding score
            for excel_trait, scrd_msubid in trait_name_mapping.items():
                if excel_trait in row:
                    score = row[excel_trait]
                    logger.info(f"Updating score for {excel_trait} (scrd_msubid = {scrd_msubid}) with score {score}")
                    
                    # Execute the SQL query to update the score for the current trait
                    cursor.execute(update_query, (float(score), scrd_msubid))
                else:
                    logger.warning(f"{excel_trait} not found in Excel row {index + 1}.")
        
        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All trait scores updated successfully.")
        
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

    return {"message": "Trait scores updated successfully for all records."}

@app.post("/insert_trait_scores_1/")
async def upload_excel(file: UploadFile = File(...)):
    logger.info("Received file upload for trait scores...")

    # Read the uploaded Excel file into pandas DataFrame, using "Manufacturing" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Manufacturing")

    # Log the column names from the Excel file to help with debugging
    logger.info(f"Excel columns: {list(excel_data.columns)}")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # SQL query for updating scores in the scoredetails table
    update_query = """
        UPDATE tbl_scoredetails 
        SET scrd_score = %s 
        WHERE scrd_msubid = %s AND scrd_secId = 1
    """

    # Define a mapping between Excel trait names and scrd_msubid
    trait_name_mapping = {
        "TRAIT_Revenue & Profit Calculation" : 1,
        "TRAIT_Financial Management " : 2,
        "TRAIT_Capital & Expenses Assessment ": 3,
        "TRAIT_People Development" : 4,
        "TRAIT_Sales & Marketing" : 5,
        "TRAIT_ Business Operation Management " : 6,
        "TRAIT_Business Strategic Management " : 7,
        "TRAIT_Vision" : 8,
        "TRAIT_Decisiveness" : 9,
        "TRAIT_Strategic Thinking" : 10,
        "TRAIT_Collaboration": 11,
        "TRAIT_Team Management " : 12,
        "TRAIT_Conscientiousness" : 13,
        "TRAIT_Agreeableness" : 14,
        "TRAIT_Extraversion" : 15,
        "TRAIT_Emotional Stability" : 16,
        "TRAIT_Humility" : 17,
        "TRAIT_Change Readiness" : 18,
        "TRAIT_Safety & Health" : 25,
        "TRAIT_Human Resources ": 26,
        "TRAIT_Research & Development " : 27,
        "TRAIT_Planning & Execution" : 28
    }

    try:
        logger.info("Executing trait score updates...")

        # Ensure that the Excel has the correct trait columns
        missing_cols = [col for col in trait_name_mapping.keys() if col not in excel_data.columns]
        if missing_cols:
            logger.error(f"Missing columns in Excel file: {missing_cols}")
            return {"error": f"Missing columns in Excel file: {missing_cols}"}
        
        # Iterate through each row in the Excel DataFrame
        for index, row in excel_data.iterrows():
            logger.info(f"Updating trait scores for record {index + 1}...")

            # Iterate through each trait name and update the corresponding score
            for excel_trait, scrd_msubid in trait_name_mapping.items():
                if excel_trait in row:
                    score = row[excel_trait]
                    
                    # Check if the score is NaN and handle it
                    if pd.isna(score):
                        logger.warning(f"Skipping NaN score for {excel_trait} (scrd_msubid = {scrd_msubid}) in row {index + 1}")
                        continue  # Skip this trait if the score is NaN
                    
                    logger.info(f"Updating score for {excel_trait} (scrd_msubid = {scrd_msubid}) with score {score}")
                    
                    # Execute the SQL query to update the score for the current trait
                    cursor.execute(update_query, (float(score), scrd_msubid))
                else:
                    logger.warning(f"{excel_trait} not found in Excel row {index + 1}.")
        
        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All trait scores updated successfully.")
        
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

    return {"message": "Trait scores updated successfully for all records."}

# GET request for retrieving updated trait scores
@app.get("/get_trait_scores_2/")
async def get_trait_scores():
    logger.info("Retrieving updated trait scores for secId = 2...")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        query = """
            SELECT scrd_secId, scrd_epId, scrd_msubid, scrd_score
            FROM tbl_scoredetails 
            WHERE scrd_secId = 2
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            logger.warning("No scores found for secId = 2.")
            return {"message": "No scores found for secId = 2."}

        logger.info("Scores retrieved successfully.")
        return {"trait_scores": results}

    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()


# GET request for retrieving updated entrepreneur emails
@app.get("/get_entrepreneur_emails_2/")
async def get_entrepreneur_emails():
    logger.info("Retrieving updated entrepreneur emails for ep_sector = 47...")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        query = """
            SELECT ep_id, ep_email 
            FROM tbl_entrepreneur 
            WHERE ep_sector = 47
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            logger.warning("No emails found for ep_sector = 47.")
            return {"message": "No emails found for ep_sector = 47."}

        logger.info("Emails retrieved successfully.")
        return {"entrepreneur_emails": results}

    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()

# GET request for retrieving updated entrepreneur names
@app.get("/get_entrepreneur_names_2/")
async def get_entrepreneur_names():
    logger.info("Retrieving updated entrepreneur names for ep_sector = 47...")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        query = """
            SELECT ep_id, ep_name 
            FROM tbl_entrepreneur 
            WHERE ep_sector = 47
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            logger.warning("No names found for ep_sector = 47.")
            return {"message": "No names found for ep_sector = 47."}

        logger.info("Names retrieved successfully.")
        return {"entrepreneur_names": results}

    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()
        
# Combined update for name, email, and trait assessmentscores for sector 47
@app.post("/combined_update_name_email_trait_assessmentscores_2/")
async def combined_update(file: UploadFile = File(...)):
    logger.info("Received file upload for combined update...")

    # Read the uploaded Excel file into pandas DataFrame
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Agribusiness")
    
    # Log the column names from the Excel file to help with debugging
    logger.info(f"Excel columns: {list(excel_data.columns)}")

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Part 1: Update entrepreneur names where ep_sector = 47
        logger.info("Starting entrepreneur names update...")

        # Fetch the rows where ep_sector = 47
        fetch_query = "SELECT ep_id FROM tbl_entrepreneur WHERE ep_sector = 47"
        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        if len(rows) < 22:
            logger.error("Not enough rows with ep_sector = 47 to update.")
            return {"error": "Not enough rows to update."}

        for index, row in excel_data.iterrows():
            if index >= 22:
                break  # Only take the first 22 names from the Excel file

            ep_name = row['Name']  # Column in Excel with names
            ep_id = rows[index][0]  # Get the ep_id of the corresponding row in the database
            logger.info(f"Updating ep_name '{ep_name}' for ep_id = {ep_id}")

            # Update query for the name
            update_query_name = """
                UPDATE tbl_entrepreneur 
                SET ep_name = %s 
                WHERE ep_id = %s AND ep_sector = 47
            """
            cursor.execute(update_query_name, (ep_name, ep_id))

        logger.info("Entrepreneur names updated successfully.")

        # Part 2: Update entrepreneur emails where ep_sector = 47
        logger.info("Starting entrepreneur emails update...")

        # Iterate through the first 22 records in the Excel sheet and update emails
        for index, row in excel_data.iterrows():
            if index >= 22:
                break  # Only take the first 22 emails from the Excel file

            ep_email = row['Email']  # Assuming the email column is named 'Email'
            ep_id = rows[index][0]  # Get the ep_id of the corresponding row in the database

            logger.info(f"Updating ep_email '{ep_email}' for ep_id = {ep_id}")

            # Update query for the email
            update_query_email = """
                UPDATE tbl_entrepreneur 
                SET ep_email = %s 
                WHERE ep_id = %s AND ep_sector = 47
            """
            cursor.execute(update_query_email, (ep_email, ep_id))

        logger.info("Entrepreneur emails updated successfully.")

        # Part 3: Update trait scores
        logger.info("Starting trait scores update...")

        trait_name_mapping = {
            "TRAIT_Revenue & Profit Calculation" : 1,
            "TRAIT_Financial Management " : 2,
            "TRAIT_Capital & Expenses Assessment ": 3,
            "TRAIT_People Development" : 4,
            "TRAIT_Sales & Marketing" : 5,
            "TRAIT_ Business Operation Management " : 6,
            "TRAIT_Business Strategic Management " : 7,
            "TRAIT_Vision" : 8,
            "TRAIT_Decisiveness" : 9,
            "TRAIT_Strategic Thinking" : 10,
            "TRAIT_Collaboration": 11,
            "TRAIT_Team Management " : 12,
            "TRAIT_Conscientiousness" : 13,
            "TRAIT_Agreeableness" : 14,
            "TRAIT_Extraversion" : 15,
            "TRAIT_Emotional Stability" : 16,
            "TRAIT_Humility" : 17,
            "TRAIT_Change Readiness" : 18,
            "TRAIT_Technology & Innovation" : 33,
            "TRAIT_Quality" : 34,
            "TRAIT_Research & Development " : 35,
            "TRAIT_Planning & Execution" : 36
        }

        update_query_score = """
            UPDATE tbl_scoredetails 
            SET scrd_score = %s 
            WHERE scrd_msubid = %s AND scrd_secId = 2
        """

        # Ensure that the Excel has the correct trait columns
        missing_cols = [col for col in trait_name_mapping.keys() if col not in excel_data.columns]
        if missing_cols:
            logger.error(f"Missing columns in Excel file: {missing_cols}")
            return {"error": f"Missing columns in Excel file: {missing_cols}"}

        # Iterate through each row in the Excel DataFrame
        for index, row in excel_data.iterrows():
            logger.info(f"Updating trait scores for record {index + 1}...")

            # Iterate through each trait name and update the corresponding score
            for excel_trait, scrd_msubid in trait_name_mapping.items():
                if excel_trait in row:
                    score = row[excel_trait]
                    logger.info(f"Updating score for {excel_trait} (scrd_msubid = {scrd_msubid}) with score {score}")
                    cursor.execute(update_query_score, (float(score), scrd_msubid))
                else:
                    logger.warning(f"{excel_trait} not found in Excel row {index + 1}.")

        logger.info("Trait scores updated successfully.")

        # Commit the updates for both operations
        connection.commit()

    except Exception as e:
        connection.rollback()
        logger.error(f"Error during database operation: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

    return {"message": "Entrepreneur names, emails, assessment and trait scores updated successfully."}

# Combined update for name, email, and trait assessmentscores for sector 46
@app.post("/combined_update_name_email_trait_assessmentscores_1/")
async def update_entrepreneur_and_trait_data(file: UploadFile = File(...)):
    logger.info("Received file upload for sector 46 (names, emails, and trait scores)...")

    # Read the uploaded Excel file into pandas DataFrame, using "Manufacturing" sheet
    contents = await file.read()
    excel_data = pd.read_excel(io.BytesIO(contents), sheet_name="Manufacturing")

    # Log the shape and column names from the Excel file to help with debugging
    logger.info(f"Excel data shape: {excel_data.shape}")
    logger.info(f"Excel columns: {list(excel_data.columns)}")

    # Check that the number of rows in the Excel file is correct
    if len(excel_data) != 114:
        logger.error(f"Expected 114 rows in the Excel file, but found {len(excel_data)}.")
        return {"error": "Excel file does not contain 114 rows."}

    # Establish MySQL connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch the IDs of the existing rows where ep_sector = 46
    cursor.execute("SELECT ep_id FROM tbl_entrepreneur WHERE ep_sector = 46")
    existing_ids = cursor.fetchall()  # Fetch all IDs for sector 46

    # Check if we fetched the expected number of IDs
    if len(existing_ids) != 114:
        logger.error(f"Expected 114 IDs for sector 46, but got {len(existing_ids)}. Existing IDs: {existing_ids}")
        return {"error": "Mismatch between Excel rows and database rows."}

    # SQL queries for updating names and emails
    update_name_query = "UPDATE tbl_entrepreneur SET ep_name = %s WHERE ep_id = %s"
    update_email_query = "UPDATE tbl_entrepreneur SET ep_email = %s WHERE ep_id = %s"

    # SQL query for updating scores in the scoredetails table
    update_score_query = """
        UPDATE tbl_scoredetails 
        SET scrd_score = %s 
        WHERE scrd_msubid = %s AND scrd_secId = 1
    """

    # Define a mapping between Excel trait names and scrd_msubid
    trait_name_mapping = {
        "TRAIT_Revenue & Profit Calculation" : 1,
        "TRAIT_Financial Management " : 2,
        "TRAIT_Capital & Expenses Assessment ": 3,
        "TRAIT_People Development" : 4,
        "TRAIT_Sales & Marketing" : 5,
        "TRAIT_ Business Operation Management " : 6,
        "TRAIT_Business Strategic Management " : 7,
        "TRAIT_Vision" : 8,
        "TRAIT_Decisiveness" : 9,
        "TRAIT_Strategic Thinking" : 10,
        "TRAIT_Collaboration": 11,
        "TRAIT_Team Management " : 12,
        "TRAIT_Conscientiousness" : 13,
        "TRAIT_Agreeableness" : 14,
        "TRAIT_Extraversion" : 15,
        "TRAIT_Emotional Stability" : 16,
        "TRAIT_Humility" : 17,
        "TRAIT_Change Readiness" : 18,
        "TRAIT_Safety & Health" : 25,
        "TRAIT_Human Resources ": 26,
        "TRAIT_Research & Development " : 27,
        "TRAIT_Planning & Execution" : 28
    }

    try:
        logger.info("Updating names, emails, and trait scores for sector 46...")

        # Iterate through each row in the Excel DataFrame to update names and emails
        for index, row in excel_data.iterrows():
            ep_id = existing_ids[index][0]  # Get the corresponding ep_id

            # Update name
            ep_name = row['Name']  # Get the name from the DataFrame
            logger.info(f"Updating name '{ep_name}' for ep_id {ep_id}...")
            cursor.execute(update_name_query, (ep_name, ep_id))  # Update the database for names

            # Update email
            ep_email = row['Email']  # Get the email from the DataFrame
            logger.info(f"Updating email '{ep_email}' for ep_id {ep_id}...")
            cursor.execute(update_email_query, (ep_email, ep_id))  # Update the database for emails

        # Ensure that the Excel has the correct trait columns
        missing_cols = [col for col in trait_name_mapping.keys() if col not in excel_data.columns]
        if missing_cols:
            logger.error(f"Missing columns in Excel file: {missing_cols}")
            return {"error": f"Missing columns in Excel file: {missing_cols}"}

        # Iterate through each row in the Excel DataFrame to update trait scores
        for index, row in excel_data.iterrows():
            logger.info(f"Updating trait scores for record {index + 1}...")

            # Iterate through each trait name and update the corresponding score
            for excel_trait, scrd_msubid in trait_name_mapping.items():
                if excel_trait in row:
                    score = row[excel_trait]

                    # Check if the score is NaN and handle it
                    if pd.isna(score):
                        logger.warning(f"Skipping NaN score for {excel_trait} (scrd_msubid = {scrd_msubid}) in row {index + 1}")
                        continue  # Skip this trait if the score is NaN

                    logger.info(f"Updating score for {excel_trait} (scrd_msubid = {scrd_msubid}) with score {score}")
                    
                    # Execute the SQL query to update the score for the current trait
                    cursor.execute(update_score_query, (float(score), scrd_msubid))

        # Commit the updates after processing all rows
        connection.commit()
        logger.info("All names, emails, and trait scores updated successfully.")

    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

    return {"message": "Names, emails, and trait scores updated successfully for all records."}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
