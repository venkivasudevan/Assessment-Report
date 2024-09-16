import pdfkit

path_to_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'

config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)

# Path of HTML file
html_file = 'index.html'
css_file = r'C:\Users\venki\Work\Compunet_Connections\Frontend\css\style.css'

# Output PDF file name
final_report = 'final_report.pdf'

# Convert HTML to PDF
pdfkit.from_file(html_file, final_report, configuration=config, css=css_file)

print(f'HTML file {html_file} has been successfully converted to {final_report}')
