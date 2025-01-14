import os
import re
from datetime import datetime

test = '''{
    "src_id" : "conff",
    "src_type" : "confluence",
    "src_sub_type" : "page",
    "data" : {
        "name" : "dsllm",
        "createid" : "sejin78.yun",
        "modifiedid" : "llm",
        "modifiedtime" : "20241218121230",
        "contents_raw" : "This is a test page",
        "trees" : [
            {
                "task_id" : "task_1",
                "operator" : "BashOperator",
                "bash_command" : "echo 'hello world'"
            }
        ],
        "tags" : ["example"],
        "add_info": {"space_id" : "aaa", "space_name":"bbb"}
    },
    "wgt_param_json": {"count": 11, "view":34}
}'''

def insert_slash_before_quotes_in_data(json_str):
    def replace_quotes(match):
        return re.sub(r'(")', r'/\1', match.group(0))
    
    # Remove newline characters and spaces
    json_str = re.sub(r'\s+', '', json_str)
    
    # Apply replace_quotes to specific parts of the JSON string
    formatted_str = re.sub(r'("data"\s*:\s*{[^}]*}|("tags"\s*:\s*\[[^\]]*\])|("add_info"\s*:\s*{[^}]*}))', replace_quotes, json_str)
    
    # Remove slashes from the "data" key value
    # formatted_str2 = re.sub(r'(/"data/")\s*:\s*(/{[^}]*})', r'///\1: ///\2', formatted_str)
    
    # Exclude the "data" key itself
    formatted_str2 = re.sub(r'(,/"data/"\s*:\s*)', r'\1', formatted_str)
    
    return formatted_str2

formatted_test = insert_slash_before_quotes_in_data(test)
formatted_str2 = re.sub(r'(/"data/"\s*:\s*)', r'data\1', formatted_test)

print(formatted_test)

def convert_datetime_format(datetime_str):
    dt = datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S:000")

text = "20241218121230"
formatted_text = convert_datetime_format(text)
print(formatted_text)

# def html_to_markdown(html_content):
#     return markdownify.markdownify(html_content, heading_style="ATX")

# def convert_html_to_markdown(html_filepath, markdown_filepath):
#     with open(html_filepath, 'r', encoding='utf-8') as html_file:
#         html_content = html_file.read()
    
#     markdown_content = html_to_markdown(html_content)
    
#     with open(markdown_filepath, 'w', encoding='utf-8') as markdown_file:
#         markdown_file.write(markdown_content)


# if __name__ == "__main__":
    # html_filepath = '/home/luemier/llm/llm/dags/data/confl.html'
    # markdown_filepath = '/home/luemier/llm/llm/dags/data/confl.md'
    # convert_html_to_markdown(html_filepath, markdown_filepath)

