import os
from dotenv import load_dotenv
import pdfkit

# Load environment variables 
load_dotenv()

# path
path_to_wkhtmltopdf = os.getenv('WKHTMLTOPDF_PATH')

# Configuring pdfkit 
config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)

# Path of HTML file and CSS
html_file = os.getenv('HTML_FILE_PATH')
css_file = os.getenv('CSS_FILE_PATH')

# Output file
final_report = 'final_report.pdf'

options = {
    'page-size': 'A4',
    'dpi': 300,
    'margin-top': '0.75in',
    'margin-right': '0.75in',
    'margin-bottom': '0.75in',
    'margin-left': '0.75in',
    'encoding': 'UTF-8',
    'enable-local-file-access': '',
    'no-outline': None,
}

# Convert HTML to PDF
pdfkit.from_file(html_file, final_report, configuration=config, css=css_file, options=options)

print(f'HTML file {html_file} has been successfully converted to {final_report}')
