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
    
def getEmbedded():
    for i in ownersCol.find({}, {"Name", "Surname", "Address", "Vehicles"}):
        print(f"{i['Name']} {i['Surname']}:\n Address: {i['Address']}\n Vehicles: {i['Vehicles']}")

def aggregation():
    agrInput = [
        {
            "$lookup":
                {
                    "from": "Vehicles",
                    "localField": "Vehicles.VehicleID",
                    "foreignField": "_id",
                    "as": "Vehicles"
                }

    }
    ]
    agrOutput = ownersCol.aggregate(agrInput)
    for owner in agrOutput:
        print("Owner: {own}\nHas vehicles:".format(own=owner['Name']+ " " +owner['Surname']))
        for car in owner['Vehicles']:
            print("{make} {model} {engineDis}cc {kw}".format(make=car['Make'], model=car['Model'], engineDis=car['Engine']['DisplacementCC'], kw=car['Engine']['Fuel']))

# Main
def main():
    dataInsert()
    getEmbedded()
    aggregation()


if __name__ == "__main__":
    main()