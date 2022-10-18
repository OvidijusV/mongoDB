import pymongo
import owners, vehicles
mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")

db = mongoClient["VehicleRegistration"]

ownersCol = db["Owners"]
vehiclesCol = db["Vehicles"]

ownersCol.drop()
vehiclesCol.drop()

# Insert data
def dataInsert():
    ownersCol.insert_many([owners.owner1, owners.owner2, owners.owner3])
    vehiclesCol.insert_many([vehicles.vehicle1, vehicles.vehicle2, vehicles.vehicle3, vehicles.vehicle4, vehicles.vehicle5])
    

# Main
def main():
    dataInsert()


if __name__ == "__main__":
    main()