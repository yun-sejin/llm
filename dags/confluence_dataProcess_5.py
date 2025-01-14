import json
import re

class DataProcessor:
    def __init__(self, info, content):
        self.info = json.loads(info)
        self.content = json.loads(content)
        self.content_list = []

    def extract_titles(self):
        for ancestor in self.content.get("ancestors", []):
            if "title" in ancestor:
                self.content_list.append(ancestor["title"])

    def add_spaceid_to_list(self):
        self.content_list.append(self.content["title"])

    def process(self):
        self.extract_titles()
        self.add_spaceid_to_list()
        return self.content_list

info = '''{
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

content = '''{
    "id": "1216789",
    "title": "title 4",
    "ancestors": [
            {
                "id": "123445",
                "title": "title 1"
            },
            {
                "id": "34567",
                "title": "title 2"
            },
            {
                "id": "78901",
                "title": "title 3"
            }
        ]
}'''

processor = DataProcessor(info, content)
result = processor.process()
print(result)


