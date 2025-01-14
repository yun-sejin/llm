import json
import re

class SpacePermissionExtractor:
    def __init__(self, data):
        self.data = data
    
    def extract_confluence_group_names(self, permissions):
        confluence_group_names = []
        for permission in permissions:
            if permission.get("type") == "spaceview" and "groupName" in permission:
                group_name = permission["groupName"]
                if group_name.startswith("Confluence"):
                    confluence_group_names.append(group_name)
        return confluence_group_names

    def extract_group_names(self, permissions):
        group_names = []
        for permission in permissions:
            if permission.get("type") == "spaceview" and "groupName" in permission:
                group_name = permission["groupName"]
                if group_name.startswith("Confluence"):
                    group_names.append(group_name)
                else:
                    match = re.search(r'\(([^)]+)\)$', group_name)
                    if match:
                        group_names.append(match.group(1))
        return group_names

    def extract_ids(self, permissions):
        ids = []
        for permission in permissions:
            if permission.get("type") == "spaceview" and "id" in permission:
                ids.append(permission["id"])
        return ids

    def extract_values(self):
        values = []
        for permission in self.data["spacepermissions"]:
            if permission["type"] == "spaceview":
                values.extend(self.extract_confluence_group_names(permission["spacepermission"]))
                values.extend(self.extract_group_names(permission["spacepermission"]))
                values.extend(self.extract_ids(permission["spacepermission"]))
        return values

data = {
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
                "groupName": "Confluence",
                "type": "spaceview"
            },
            {
                "groupName": "Confluence_dev",
                "type": "spaceview"
            },
            {
                "groupName": "Confluence_etc",
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
}

extractor = SpacePermissionExtractor(data)
values = extractor.extract_values()
print(values)
