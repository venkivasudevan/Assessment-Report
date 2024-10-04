# Assessment and Entrepreneur Data Upload API

This FastAPI project allows users to upload Excel files containing assessment scores and entrepreneur names. The data is processed and inserted into a MySQL database. The API is designed for sectors like `Manufacturing`, `Agribusiness`, and different assessments such as `Financial Health`, `Business Competency`, and others.

## Features

- Upload Excel files for entrepreneur names and assessment scores.
- Data validation and mapping from Excel to MySQL tables.
- Retrieve updated trait scores and entrepreneur data.
- Logging for debugging and tracking operations.

## Project Structure

 - main.py (FastAPI main entry point)
 - requirements.txt (Python dependencies)
 - README.md (Project documentation)
 - api_requests.json (API documnetation)


## Prerequisites

Before running the application, make sure you have the following installed:

 - Python 3.7+
 - MySQL database
 - Pip (Python package installer)

## Clone the Repository

 - git clone https://github.com/your-username/your-repo.git
 - cd your-repo

## Create Virtual Environment

 - python -m venv env
 - source env/bin/activate  # For Windows: env\Scripts\activate

## Install Dependencies

 - pip install -r requirements.txt

## Database Credentials

Create a .env file in the root directory of your project and add the following content (replace with your actual database credentials):

 - DB_HOST=localhost
 - DB_USER=root
 - DB_PASSWORD=your_password_here
 - DB_NAME=assessment

## Run the Application

 - uvicorn main:app --reload

## API Documnetation

The API can be tested using Postman. Import the collection using the following steps:

1. Open Postman.
2. Check the `api_requests.json` file.
3. You can now view and test the API endpoints one by one.

