import pymongo
import owners, vehicles

mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")

db = mongoClient["VehicleRegistration"]

ownersCol = db["Owners"]
vehiclesCol = db["Vehicles"]

ownersCol.drop()
vehiclesCol.drop()
db["vehicleMap"].drop()
db["ownerMap"].drop()

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

def mapreduce():

    mapperOwner = """
        function(){
            emit(this._id, {ownerID: this._id, Name: this.Name, Surname: this.Surname, Vehicles: this.Vehicles})
        }
    """

    reducerOwner = """
        function(key, values){
            let results = {}
            results.owner = values[0];
            return results;
        }
    """

    mapperVehicle = """
        function(){
            emit(this._id, {vehicleID: this._id, Make: this.Make, Model: this.Model, engineCC: this.Engine.DisplacementCC, Fuel: this.Engine.Fuel})
        }
    """

    reducerVehicle = """
        function(key, values){
            let results = {}
            results.vehicle = values;
            return results;
        }
    """

    db.command('mapReduce', 'Owners', map=mapperOwner, reduce=reducerOwner, out={'reduce': 'ownerMap'})
    db.command('mapReduce', 'Vehicles', map=mapperVehicle, reduce=reducerVehicle, out={'reduce': 'vehicleMap'})

    for i in db["ownerMap"].find({}, {"value"}):
        print(f"Owner: {i['value']['owner']['Name']} {i['value']['owner']['Surname']}\nHas vehicles:")
        for carID in i['value']['owner']['Vehicles']:
            car = db["vehicleMap"].find_one({"_id": carID['VehicleID']})
            print(f"{car['value']['vehicle'][0]['Make']} {car['value']['vehicle'][0]['Model']} {int(car['value']['vehicle'][0]['engineCC'])}cc {car['value']['vehicle'][0]['Fuel']}")
            #print(carID['VehicleID'])

 
# Main
def main():
    dataInsert()
    print("Embedded output:")
    getEmbedded()
    print("\nAggregation output:")
    aggregation()
    print("\nMap-reduce output:")
    mapreduce()


if __name__ == "__main__":
    main()