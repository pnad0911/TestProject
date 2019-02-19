import json
from pprint import pprint


class HashMap:
    dict
    def __init__(self):
        self.dict = {}

    def add(self, key, value):
        if key not in self.dict and key != "":
            inner = [value]
            self.dict[key] = inner
        elif key in self.dict:
            if value not in self.dict[key] and value != "":
                self.dict[key].append(value)

    def get(self, key):
        if key in dict:
            return dict[key]
        else:
            return None
        
    def print(self):
        for k in self.dict:
            print(k + ":")
            for v in self.dict[k]:
                print("    " + v)

class Table:
    table
    def __init__(self,aMap,bMap):
        self.table = {}
        merge(aMap,bMap)

map = HashMap()
map1 = HashMap()
for f in open('testDataSet.json','r'):
    data = json.loads(f)
    if 'category' in data:
        if 'subcategory' in data:
            map.add(data["category"], data["subcategory"])
    if 'subcategory' in data:
        if 'product_type' in data:
            map1.add(data["subcategory"], data["product_type"])
            
map1.print()
