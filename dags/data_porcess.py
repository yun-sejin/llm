import json
import re

class DataProcessor:
    def __init__(self, data, data2):
        self.data = json.loads(data)
        self.data2 = json.loads(data2)
        self.data2_list = []

    def extract_titles(self):
        for ancestor in self.data2.get("ancestors", []):
            if "title" in ancestor:
                self.data2_list.append(ancestor["title"])

    def add_spaceid_to_list(self):
        self.data2_list.insert(0, self.data["spaceid"])

    def process(self):
        self.extract_titles()
        self.add_spaceid_to_list()
        return self.data2_list

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

data2 = '''{
    "spaceid": "6789",
    "spacename": "space 2",
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

processor = DataProcessor(data, data2)
result = processor.process()
print(result)


