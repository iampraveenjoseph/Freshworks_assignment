import json
import logging
import os
import datetime
import dateutil.parser
from filelock import FileLock

#logging.basicConfig(
    #level=logging.INFO,
    #format='%(asctime)s | %(process)d | %(levelname)s | %(message)s'
#)

class Db:
    #logging.basicConfig(
     #  level=logging.INFO,
    #format='%(asctime)s | %(process)d | %(levelname)s | %(message)s'
    #)
    def __init__(self, db_file_path):
        
        self.db_file_path = db_file_path
        self.process_lock_path = '{}.lock'.format(db_file_path)
        self.process_lock = FileLock(self.process_lock_path, timeout=-1)
    #lockpath
  
    #check if file is present or not
    def checkPath(self,key,value):
        
        with self.process_lock:
            if not os.path.exists(self.db_file_path):
                with open(self.db_file_path, 'w') as db_file:
                    db_file.write(json.dumps({}))
                self.addmultiple(key,value)    
            else:
                return True
    
    #create a empty database
    def create_db(self):
        with self.process_lock:
            if not os.path.exists(self.db_file_path):
                with open(self.db_file_path, 'w') as db_file:
                    db_file.write(json.dumps({}))
                print("Database created Successfully ")
            else:
                print("A file has Same name as your Database\n")


    #add a key value pair
    def add(self,key,value):
        if self.checkPath(key,value):
            with self.process_lock:
                with open(self.db_file_path, 'r') as json_file:
                    json_decoded = json.load(json_file)
                    if key.upper() not in json_decoded:
                        json_decoded[key.upper()] = value
                       
                    else:
                        print("Key already used \n")
                if os.path.getsize(self.db_file_path)>=1000000000:
                    print("DB reaches it's limit of 1GB\n")
                    
                with open(self.db_file_path, 'w') as json_out_file:
                    json.dump(json_decoded, json_out_file)
                print("Data added to the database ")
                
    
    #jsonvalue as value
    def addmultiple(self,key,count):
        valuedict=dict()
        if self.checkPath(key,count):
             with self.process_lock:
                with open(self.db_file_path, 'r') as json_file:
                    json_decoded = json.load(json_file)
                    if key.upper() not in json_decoded:
                        
                        for i in range(count):
                            insidekey = str(input("Enter {} key :".format(i+1)))
                            insidevalue = str(input("Enter {} value :".format(i+1)))
                            valuedict[insidekey.upper()] = insidevalue.upper()
      
        time_to_live_choice = False
        while time_to_live_choice != True:
            choice = str(input("Do you want to remove the data autonatically(y/n) :"))
            limit=0
            if choice == 'y' or 'Y':
                limit  = str(input("Enter the no of hour the data need to be store :"))
                datastored_time = datetime.datetime.now()
                datastore_time= str(datastored_time.isoformat())
                time=float((datastore_time[11:16]).replace(":","."))
                valuedict["STORED TIME"] = datastore_time[11:16]
                valuedict["EXPIRED TIME"] = str(float(limit)+time).replace(".",":")
                print(valuedict['STORED TIME'])
                time_to_live_choice = True
                return self.add(key,valuedict)

            
            elif choice == 'n' or 'N':
                time_to_live_choice = True
                return self.add(key,valuedict)
            
            else:
                time_to_live_choice = False




        
        
        valuelen =len(str(valuedict))
        #print(valuelen)
        if valuelen>16385:#16kilobytes
            print("Value Size is more than 16kb")
        
        self.add(key,valuedict)
       


    #delete a key value pair
    def delete(self,key):
        if os.path.exists(self.db_file_path):
           
            with self.process_lock:

                with open(self.db_file_path, 'r') as json_file:
                    json_decoded = json.load(json_file)
                    if key.upper() in json_decoded:
                        del json_decoded[key.upper()]
                    else:
                        print("Key not Found")
                    #print(json_decoded)
                with open(self.db_file_path, 'w') as json_out_file:
                    json.dump(json_decoded, json_out_file)
                print("Key deleted Successfully ")
        else:
            print("File Not Found")

    #remove json on time to live on expired

    def time_to_live(self):
        with self.process_lock:

            with open(self.db_file_path, 'r') as json_file:
                json_decoded = json.load(json_file)
                for key in json_decoded:
                    keyValue = json_decoded[key]
                    
                    expired = float(str(keyValue["EXPIRED TIME"]).replace(":","."))
                    limit = datetime.datetime.now() 
                    check_time= str(limit.isoformat())
                    time=float((check_time[11:16]).replace(":","."))
                   # print(key)
                    if time >= expired:
                        self.delete(key)
                        print("Because the time expired")

                        
                    else:
                        return True

                    
            


    #display the json
    def display(self):
        if os.path.exists(self.db_file_path):
           
            with self.process_lock:
                
                filename=os.path.basename(self.db_file_path)   
                with open(filename,"r") as file_name:
                    json_decoded = json.load(file_name)
                    print(json_decoded)
                    print("\n")
                
        else:
            print("Db is empty")

    #retrive specific data
    def retrive(self,key):
        if os.path.exists(self.db_file_path):
            with self.process_lock:
                with open(self.db_file_path, 'r') as json_file:
                    json_decoded = json.load(json_file)
                    if key.upper() in json_decoded:
                        print(json_decoded[key.upper()])
                    else:
                        print("No key found in name of {}" .format(key))
    

    #display full database

    def displayall(self):
        if os.path.exists(self.db_file_path):
            with self.process_lock:
                with open(self.db_file_path, 'r') as json_file:
                    json_decoded = json.load(json_file)
                    print(json_decoded)
        else:
            print("File not found ")



    #lockpath("F:/json/user.json")
    #checkPath("F:/json/user.json")
    #add("ama","s2",'F:/json/user.json')
    #delete("ama",'F:/json/user.json')
    #removefile("F:/json/user.json")
    #display("F:/json/user.json")
#database = Db("F:/json/user.json")
#database.create_db()
#database.addmultiple("google",1)
#database.display()
#database.time_to_live()
#database.retrive("as")



print("\t\t\t\t\t\t WELCOME TO JSON DATABASE\n")

print("AVAILABLE OPERATION\n")

print("1.Create a new database\n")
print('2.Add key value pair in the database\n')
print('3.Remove key value pair in the database\n')
print("4.Display value of a key in the database\n")
print("5.Display all element in the database\n")



option=str(input("Enter your choice :"))

print("Give input as 1,2,3,4,5\n")
if option =="1":#create a database
    locationlocked = False


    while locationlocked == False:
        loca = str(input(("Do you want to store in default location(y/n) :")))

        if(loca == 'y' or loca == 'Y'):
            fullpath = os.getcwd()
            filename = os.path.basename(fullpath)
            
        # print(default_location.replace('\\',"/"))
            db_name = str(input("Enter the database name : "))
            database=Db(fullpath+db_name+".json")
            locationlocked = True
        elif(loca=="n" or loca == "N"):
            try:
                location=str(input("Enter the path where you want to store Your database : "))
            except:
                print("Specify a path[eg:F:/jsondb]\n Try again")
            db_name= str(input("Enter the database name : "))
            
            database=Db(location+db_name+".json")
            locationlocked = True
        else:
            print("Please Enter Valid Response(y/n) ")
            locationlocked = False
    database.create_db()
    
    
elif option == "2":
    try:
        location = str(input("Enter the Location of the database : "))
    except:
        print("Specify a path[eg:F:/jsondb\db.json]\n Try again")
    key = str(input("Enter the key value : "))
    try:
        count = int(input("Enter number of key value pair(count) : "))
    except:
        print("Above response is invalid please give valid response ")
    database = Db(location)
    database.addmultiple(key,count)
    

elif option == "3":
    try:
        location = str(input("Enter the Location of the database : "))
    except:
        print("Specify a path[eg:F:/jsondb\db.json]\n Try again")
    key = str(input("Enter the key value : "))
    database = Db(location)

    database.delete(key)
    

elif option == "4":
    try:
        location = str(input("Enter the Location of the database : "))
    except:
        print("Specify a path[eg:F:/jsondb\db.json]\n Try again")
    key = str(input("Enter the key value : "))
    database = Db(location)

    database.retrive(key)
    

elif option == "5":
    try:
        location = str(input("Enter the Location of the database : "))
    except:
        print("Specify a path[eg:F:/jsondb\db.json]\n Try again")
    
    database = Db(location)

    database.displayall()
    
    

else:
    print("Enter available option only\n")








