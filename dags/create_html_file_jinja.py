import os
from jinja2 import Environment, FileSystemLoader

def create_html_file(message, file_name, folder_path='fail', table_width='100%', div_id='table-container'):
    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)
    
    # Define the full file path
    file_path = os.path.join(folder_path, file_name)
    
    # Load the Jinja template
    env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)))
    template = env.get_template('template.html.jinja')
    
    # Render the template with the message content
    html_content = template.render(
        title='Error Report',
        heading='Error Details',
        rows=[{'cell1': item['src_id'], 'cell2': item['error'], 'cell3': item['etc']} for item in message]
    )
    
    # Write the HTML content to the file
    with open(file_path, 'w', encoding='utf-8') as html_file:
        html_file.write(html_content)
    
    print(f'HTML file created at {file_path}')
    return file_path

# Example usage
message = [{
    "src_id": "qna_dsqna_1.",
    "error": "Another error occurred1.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    },{
    "src_id": "qna_dsqna_2.",
    "error": "Another error occurred2.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    },{
    "src_id": "qna_dsqna_3.",
    "error": "Another error occurred3.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    }]
file_name = 'error_message.html'

create_html_file(message, file_name, table_width='80%', div_id='error-table')
