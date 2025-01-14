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
    # Remove newline characters
    # json_str = json_str.replace('\n', '')
    
    # Exclude the "data" key itself
    json_str = re.sub(r'("data"\s*:\s*)', r'\1', json_str)
    return re.sub(r'("data"\s*:\s*{[^}]*}|("tags"\s*:\s*\[[^\]]*\])|("add_info"\s*:\s*{[^}]*}))', replace_quotes, json_str)

formatted_test = insert_slash_before_quotes_in_data(test)
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



은 정규 표현식 매칭 결과에서 전체 매칭된 문자열을 반환합니다. match 객체는 re 모듈의 search, match, findall 등의 함수로부터 반환된 매칭 결과를 나타내는 객체입니다.

예제 코드 설명
아래는 insert_slash_before_quotes_in_data 함수에서 replace_quotes 함수가 어떻게 동작하는지 설명합니다.

file:///home/luemier/llm/llm/dags/confluence_text copy.py

import re

def insert_slash_before_quotes_in_data(json_str):
    def replace_quotes(match):
        # match.group(0)은 전체 매칭된 문자열을 반환합니다.
        return re.sub(r'(")', r'/\1', match.group(0))
    
    # Remove newline characters and spaces
    json_str = re.sub(r'\s+', '', json_str)
    
    # Exclude the "data" key itself
    json_str = re.sub(r'("data"\s*:\s*)', r'\1', json_str)
    
    # Apply replace_quotes to specific parts of the JSON string
    return re.sub(r'("data"\s*:\s*{[^}]*}|("tags"\s*:\s*\[[^\]]*\])|("add_info"\s*:\s*{[^}]*}))', replace_quotes, json_str)

test = '''{
    "data": {
        "id": 123,
        "name": "example",
        "tags": ["tag1", "tag2"],
        "add_info": {"space_id" : "aaa", "space_name":"bbb"}
    },
    "wgt_param_json": {"count": 11, "view":34}
}'''

formatted_test = insert_slash_before_quotes_in_data(test)
print(formatted_test)

설명
replace_quotes 함수:

replace_quotes 함수는 re.sub 함수의 replacement 인자로 사용됩니다.
match 객체는 정규 표현식 매칭 결과를 나타내며, match.group(0)은 전체 매칭된 문자열을 반환합니다.
re.sub(r'(")', r'/\1', match.group(0))는 매칭된 문자열에서 모든 큰따옴표(") 앞에 슬래시(/)를 추가합니다.
insert_slash_before_quotes_in_data 함수:

json_str에서 모든 공백과 줄바꿈 문자를 제거합니다.
"data" 키 자체를 제외하고, 특정 부분("data", "tags", "add_info")에 대해 replace_quotes 함수를 적용합니다.
test 문자열:

예제 JSON 문자열입니다.
insert_slash_before_quotes_in_data 함수를 사용하여 특정 부분의 큰따옴표 앞에 슬래시를 추가합니다.
출력:

formatted_test는 변환된 JSON 문자열을 출력합니다.
match.group 사용 예제

import re

pattern = r'(\d+)-(\d+)-(\d+)'
text = '2023-10-05'

match = re.search(pattern, text)
if match:
    print(match.group(0))  # 전체 매칭된 문자열: '2023-10-05'
    print(match.group(1))  # 첫 번째 그룹: '2023'
    print(match.group(2))  # 두 번째 그룹: '10'
    print(match.group(3))  # 세 번째 그룹: '05'

이 예제에서 match.group(0)은 전체 매칭된 문자열을, match.group(1), match.group(2), match.group(3)은 각각 첫 번째, 두 번째, 세 번째 그룹을 반환합니다.




//////////////////


re.sub는 Python의 re 모듈에서 제공하는 함수로, 정규 표현식을 사용하여 문자열 내의 패턴을 다른 문자열로 대체하는 데 사용됩니다. re.sub 함수는 다음과 같은 형식으로 사용됩니다:


re.sub(pattern, repl, string, count=0, flags=0)

매개변수
pattern: 대체할 패턴을 나타내는 정규 표현식.
repl: 대체할 문자열 또는 함수.
string: 입력 문자열.
count: 대체할 최대 횟수 (기본값은 0으로, 모든 패턴을 대체).
flags: 정규 표현식 플래그 (예: re.IGNORECASE).
반환값
패턴이 대체된 새로운 문자열을 반환합니다.
예제
아래는 re.sub의 사용 예제입니다.

기본 사용 예제

import re

text = "The rain in Spain"
pattern = r"ain"
replacement = "123"

result = re.sub(pattern, replacement, text)
print(result)  # 출력: "The r123 in Sp123"

import re

def replace_function(match):
    return match.group(0).upper()

text = "The rain in Spain"
pattern = r"ain"

result = re.sub(pattern, replace_function, text)
print(result)  # 출력: "The rAIN in SpAIN"


re.sub를 사용한 코드 예제
아래는 re.sub를 사용하여 JSON 문자열에서 특정 부분의 큰따옴표 앞에 슬래시를 추가하는 예제입니다.

file:///home/luemier/llm/llm/dags/confluence_text copy.py

import re

def insert_slash_before_quotes_in_data(json_str):
    def replace_quotes(match):
        return re.sub(r'(")', r'/\1', match.group(0))
    
    # Remove newline characters and spaces
    json_str = re.sub(r'\s+', '', json_str)
    
    # Exclude the "data" key itself
    json_str = re.sub(r'("data"\s*:\s*)', r'\1', json_str)
    
    # Apply replace_quotes to specific parts of the JSON string
    return re.sub(r'("data"\s*:\s*{[^}]*}|("tags"\s*:\s*\[[^\]]*\])|("add_info"\s*:\s*{[^}]*}))', replace_quotes, json_str)

test = '''{
    "data": {
        "id": 123,
        "name": "example",
        "tags": ["tag1", "tag2"],
        "add_info": {"space_id" : "aaa", "space_name":"bbb"}
    },
    "wgt_param_json": {"count": 11, "view":34}
}'''

formatted_test = insert_slash_before_quotes_in_data(test)
print(formatted_test)

설명
replace_quotes 함수:

re.sub(r'(")', r'/\1', match.group(0))는 매칭된 문자열에서 모든 큰따옴표(") 앞에 슬래시(/)를 추가합니다.
match.group(0)은 전체 매칭된 문자열을 반환합니다.
insert_slash_before_quotes_in_data 함수:

re.sub(r'\s+', '', json_str)를 사용하여 json_str에서 모든 공백과 줄바꿈 문자를 제거합니다.
re.sub(r'("data"\s*:\s*)', r'\1', json_str)를 사용하여 "data" 키 자체를 제외합니다.
re.sub(r'("data"\s*:\s*{[^}]*}|("tags"\s*:\s*\[[^\]]*\])|("add_info"\s*:\s*{[^}]*}))', replace_quotes, json_str)를 사용하여 특정 부분의 큰따옴표 앞에 슬래시를 추가합니다.
이 예제는 re.sub를 사용하여 JSON 문자열에서 특정 부분의 큰따옴표 앞에 슬래시를 추가하는 방법을 보여줍니다.

<html


//////////////////



의 결과값은 정규 표현식에서 두 번째 그룹에 해당하는 부분 문자열을 반환합니다. 두 번째 그룹은 정규 표현식에서 두 번째로 괄호로 묶인 부분을 의미합니다.

예제 코드
아래는 re.sub를 사용하여 JSON 문자열에서 특정 부분의 두 번째 그룹을 대체하는 예제입니다.

file:///home/luemier/llm/llm/dags/confluence_text copy.py

import re

def insert_slash_before_quotes_in_data(json_str):
    def replace_quotes(match):
        # match.group(2)은 두 번째 그룹을 반환합니다.
        return re.sub(r'(")', r'/\1', match.group(2))
    
    # Remove newline characters and spaces
    json_str = re.sub(r'\s+', '', json_str)
    
    # Exclude the "data" key itself
    json_str = re.sub(r'("data"\s*:\s*)', r'\1', json_str)
    
    # Apply replace_quotes to specific parts of the JSON string
    return re.sub(r'("data"\s*:\s*{([^}]*)}|("tags"\s*:\s*\[([^\]]*)\])|("add_info"\s*:\s*{([^}]*)}))', replace_quotes, json_str)

test = '''{
    "data": {
        "id": 123,
        "name": "example",
        "tags": ["tag1", "tag2"],
        "add_info": {"space_id" : "aaa", "space_name":"bbb"}
    },
    "wgt_param_json": {"count": 11, "view":34}
}'''

formatted_test = insert_slash_before_quotes_in_data(test)
print(formatted_test)


설명
replace_quotes 함수:

replace_quotes 함수는 re.sub 함수의 replacement 인자로 사용됩니다.
match 객체는 정규 표현식 매칭 결과를 나타내며, match.group(2)은 두 번째 그룹을 반환합니다.
re.sub(r'(")', r'/\1', match.group(2))는 두 번째 그룹에서 모든 큰따옴표(") 앞에 슬래시(/)를 추가합니다.
insert_slash_before_quotes_in_data 함수:

re.sub(r'\s+', '', json_str)를 사용하여 json_str에서 모든 공백과 줄바꿈 문자를 제거합니다.
re.sub(r'("data"\s*:\s*)', r'\1', json_str)를 사용하여 "data" 키 자체를 제외합니다.
re.sub(r'("data"\s*:\s*{([^}]*)}|("tags"\s*:\s*\[([^\]]*)\])|("add_info"\s*:\s*{([^}]*)}))', replace_quotes, json_str)를 사용하여 특정 부분의 두 번째 그룹에서 큰따옴표 앞에 슬래시를 추가합니다.
출력:

formatted_test는 변환된 JSON 문자열을 출력합니다.
match.group(2)의 결과값
match.group(2)의 결과값은 정규 표현식에서 두 번째 그룹에 해당하는 부분 문자열입니다. 예를 들어, 아래와 같은 정규 표현식과 문자열이 있을 때:


pattern = r'("data"\s*:\s*{([^}]*)}|("tags"\s*:\s*\[([^\]]*)\])|("add_info"\s*:\s*{([^}]*)}))'
text = '{"data": {"id": 123, "name": "example", "tags": ["tag1", "tag2"], "add_info": {"space_id" : "aaa", "space_name":"bbb"}}}'


이 정규 표현식에서 match.group(2)는 "data" 키의 값 부분을 반환합니다. 예를 들어, {"id": 123, "name": "example", "tags": ["tag1", "tag2"], "add_info": {"space_id" : "aaa", "space_name":"bbb"}} 부분이 됩니다.

이 예제는 re.sub를 사용하여 JSON 문자열에서 특정 부분의 두 번째 그룹에서 큰따옴표 앞에 슬래시를 추가하는 방법을 보여줍니다. match.group(2)을 사용하여 매칭된 두 번째 그룹을 활용하여 대체 문자열을 생성합니다.

<html


