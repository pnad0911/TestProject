import json
from pymongo import MongoClient
from bson.json_util import loads
import pprint
from bson.son import SON

#------------------ Db Class ----------------------------------------------------------------
# Handle all operation on database

class Db:
    client = None
    db = None
    collection = None
    nameDB = None

    #Initialize the database, collection
    def __init__(self,nameDB, nameCollection, pathJsonFile):
        self.client = MongoClient('localhost', 27017)
        self.client.drop_database(nameDB)
        self.nameDB = nameDB
        self.db = self.client[nameDB]
        self.collection = self.db[nameCollection]
        with open(pathJsonFile, 'rb') as f:
            for row in f:
               data = loads(row)
               self.collection.insert_one(data)
               
    #Destructor -- delete db and collection
    def __del__(self):
        self.collection.drop()
        self.client.drop_database(self.nameDB)
        self.client.close()
        
    # aggregate   
    def aggregate(self, s):
        return self.collection.aggregate(s)


#------------------ myPrint Class ----------------------------------------------------------------
# Handle write the result to file

class myPrint:
    kCatSize = 0
    kIsSize = 0
    f = 0

    #Initialize file and the size of each box
    def __init__(self,kCatSize, kIsSize, resFile):
        self.kIsSize = kIsSize
        self.kCatSize = kCatSize
        self.f = open(resFile, "w")
        
    #Destructor
    def __del__(self):
        self.f.close()

    #Print box
    def printHead(self):
        line1 = ""
        line2 = ""
        for i in range(self.kCatSize):
            line1+='-'
        for i in range(self.kIsSize):
            line2+='-'
        self.f.write(" {}{}{}{}{}{}{}{}{}{}{}{}{}\n".format(line1,'+',line1,'+',line1,'+',line2,'+',line2,'+',line2,'+',line2))

    #Print body part
    def printBody(self,s1,s2,s3,s4,s5,s6,s7):
        ms1 = self.cal(s1,True)
        ms2 = self.cal(s2,True)
        ms3 = self.cal(s3,True)
        ms4 = self.cal(s4,False)
        ms5 = self.cal(s5,False)
        ms6 = self.cal(s6,False)
        ms7 = self.cal(s7,False)
        self.f.write("|{}|{}|{}|{}|{}|{}|{}|\n".format(ms1,ms2,ms3,ms4,ms5,ms6,ms7))

    #Positioning the text
    def cal(self,s,isCat):
        ms = "";
        if isCat:
            indent = (self.kCatSize-len(s))//2
            for i in range(indent):
                ms += ' '
            ms +=s
            for i in range(self.kCatSize-len(s)-indent):
                ms += ' '
        else:
            indent = (self.kIsSize-len(s))//2
            for i in range(indent):
                ms += ' '
            ms +=s
            for i in range(self.kIsSize-len(s)-indent):
                ms += ' '
        return ms


#------------------ Main ----------------------------------------------------------------
    
my = Db('test','test','testDataSet.json')  #Initialize the database object

percent = 0.8  #Set the probability

#The mongodb code 
typeC = [{"$group": {"_id": {"cat": "$category", "sub": "$subcategory", "type":"$product_type"},
                      "num_baby": {"$sum": {"$cond": ["$is_baby",1,0]}},
                      "num_men": {"$sum": {"$cond": ["$is_men",1,0]}},
                      "num_mother": {"$sum": {"$cond": ["$is_mother",1,0]}},
                      "num_night": {"$sum": {"$cond": ["$is_night_care",1,0]}},
                      "total": {"$sum": 1}}},
         {"$project": {"is_baby": {"$gt":[{"$divide":["$num_baby","$total"]},percent] }, "is_men": {"$gt":[{"$divide":["$num_men","$total"]},percent] },
                       "is_mother": {"$gt":[{"$divide":["$num_mother","$total"]},percent] }, "is_night": {"$gt":[{"$divide":["$num_night","$total"]},percent] }}},
         {"$match" : {"_id.type": {"$ne": '',"$exists":True, "$ne":None}}},
         {"$sort": {"_id.cat":1, "_id.sub":1, "_id.type":1}}]

typ = my.aggregate(typeC)

p = myPrint(30,20,"Result.txt")  #Initialize the myPrint object

#write the result and json
p.printHead()
p.printBody("Category", "Subcategory","Product Type", "Kid's & Baby Flag", "Men Flag", "Mother Flag", "Night Care Flag")
p.printHead()
y = 'Y'
pCat = None
pSub = None

f = open("data.json", "w")
for item in typ:
    ele = item['_id']
    if ele['type']:  #Only write if the lowest field [product_type] has a name exists
        json.dump(item, f)
        f.write('\n')
        baby,men,mom,night = ('',)*4
        if item['is_baby']:
            baby = y
        if item['is_men']:
            men = y
        if item['is_mother']:
            mom = y
        if item['is_night']:
            night = y
        if pSub != ele['sub']:
            if pCat == ele['cat']:
                p.printBody("",ele['sub'],ele['type'],baby, men, mom,night)
            else:
                p.printHead()
                p.printBody(ele['cat'],ele['sub'],ele['type'],baby, men, mom,night)
        else:
            p.printBody("","",ele['type'],baby, men, mom,night)
        pSub = ele['sub']
        pCat = ele['cat']

p.printHead()
f.close()


