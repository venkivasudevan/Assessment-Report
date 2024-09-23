import pdfkit

# Absolute path to wkhtmltopdf
path_to_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'

# Configuring pdfkit with wkhtmltopdf
config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)

# Path of HTML file and CSS
html_file = r'C:\Users\venki\Work\Compunet_Connections\Frontend\index.html'
css_file = r'C:\Users\venki\Work\Compunet_Connections\Frontend\css\style.css'

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
    'enable-local-file-access': '',  # This allows local file access
}

# Convert HTML to PDF
pdfkit.from_file(html_file, final_report, configuration=config, css=css_file, options=options)

print(f'HTML file {html_file} has been successfully converted to {final_report}')
