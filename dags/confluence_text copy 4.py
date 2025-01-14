import json
import re

class SpacePermissionExtractor:
    def __init__(self, data):
        self.data = json.loads(data)
    
    def extract_values(self):
        values = []
        for permission in self.data["spacepermissions"]:
            if permission["type"] == "spaceview":
                for sub_permission in permission["spacepermission"]:
                    if sub_permission.get("type") == "spaceview":
                        if "groupName" in sub_permission:
                            match = re.search(r'\(([^)]+)\)$', sub_permission["groupName"])
                            if match:
                                values.append(match.group(1))
                        if "id" in sub_permission:
                            values.append(sub_permission["id"])
        return values

data = '''{
    "spaceid": "6789",
    "spacename": "space 2",
    "spacepermissions": [{
        "spacepermission" : [
            {
                "groupName": "Data platform(ai센터)(78082212345)",
                "type": "spaceview"
            },
            {
                "groupName": "SSG_PLATFORM(32345653566)",
                "type": "spaceview"
            },
            {
                "groupName": "Confluence(23231322323)",
                "type": "spaceview"
            },
            {
                "groupName": "Confluence_dev(88888888888)",
                "type": "spaceview"
            },
            {
                "groupName": "Confluence_etc(88888888888)",
                "type": "spaceview"
            },
            {
                "id": "sejin7.yun",
                "type": "spaceview"
            },
            {
                "id": "jiyu.yun",
                "type": "spaceview"
            }
        ],
           "type": "spaceview" 
         },{
         "spacepermission" : [
            {
                "groupName": "43567",
                "type": "spacewrite"
            },
            {
                "groupName": "123211",
                "type": "spacewrite"
            }
        ],
        "type": "spacewrite"
          }]
}'''

extractor = SpacePermissionExtractor(data)
values = extractor.extract_values()
print(values)




# ````````````````````````````````

# re.search(r'(([^)]+))$', "Data platform(ai센터)(78082212345)")
# 의미

# 정규 표현식 r'\(([^)]+)\)$'는 문자열의 끝에서 괄호 안에 있는 숫자 또는 문자를 추출하는 데 사용됩니다. 이 정규 표현식은 다음과 같은 의미를 가집니다:

# \(: 여는 괄호 (를 찾습니다.
# ([^)]+): 닫는 괄호 )가 아닌 하나 이상의 문자(숫자 포함)를 찾습니다. 이 부분은 캡처 그룹으로, 나중에 match.group(1)로 접근할 수 있습니다.
# \): 닫는 괄호 )를 찾습니다.
# $: 문자열의 끝을 나타냅니다.

# 예제 코드
# 아래는 정규 표현식을 사용하여 문자열에서 괄호 안의 숫자를 추출하는 예제입니다.

# file:///home/luemier/llm/llm/dags/confluence_text copy 4.py

# import re

# # 예제 문자열
# text = "Data platform(ai센터)(78082212345)"

# # 정규 표현식을 사용하여 괄호 안의 숫자를 추출
# match = re.search(r'\(([^)]+)\)$', text)

# if match:
#     # match.group(1)은 첫 번째 캡처 그룹을 반환합니다.
#     result = match.group(1)
#     print(result)  # 출력: 78082212345
# else:
#     print("No match found")
    
#  설명
# 정규 표현식:

# r'\(([^)]+)\)$'는 문자열의 끝에서 괄호 안에 있는 숫자 또는 문자를 찾습니다.
# [re.search](llm/dags/confluence_text copy 4.py ) 함수:

# [re.search(r'\(([^)]+)\)$', text)](llm/dags/confluence_text copy 4.py )를 사용하여 text 문자열에서 패턴을 찾습니다.
# match 객체는 매칭 결과를 나타냅니다.
# 매칭 결과 확인:

# if match: 조건문을 사용하여 매칭 결과가 있는지 확인합니다.
# match.group(1)은 첫 번째 캡처 그룹을 반환합니다. 여기서는 78082212345가 반환됩니다.
# 출력:

# print(result)는 추출된 숫자를 출력합니다.
# 출력 결과는 78082212345입니다.
# 이 예제는 정규 표현식을 사용하여 문자열의 끝에서 괄호 안에 있는 숫자를 추출하는 방법을 보여줍니다. match.group(1)을 사용하여 첫 번째 캡처 그룹을 반환합니다.

   