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

def mapreduce(ownerID):
    mapper1 = """
        function(){
            for(var i = 0; i < this.Vehicles.length; i++)
                emit(this.Vehicles[i], {VehicleID: this._id});
        }
    """
    mapper2 = """
        function(){
            emit(this._id, {Name: this.Name})
        }
    """

    reduceLookUp1 = """
        function(key, values){
            var results = {};
            var vehicles = [];
            values.forEach(function(value){
                var vehicle = {};
                if (value.VehicleID !== undefined) vehicle["VehicleID"] = value.VehicleID;
                if (Object.keys(vehicle).length > 0) vehicles.push(vehicle);
                if (value.Name !== undefined) results["Name"] = value.Name;
                if (value.vehicles !== undefined) results["vehicles"] = value.vehicles;
            });
            if (Object.keys(vehicles).length > 0) results["vehicles"] = vehicles;
            return results;
        }
    """

    mapper = """
        function() {{
        if (this.OwnerID == '""" + ownerID + """')
            emit(this.Make, this.Model, this.Engine.DisplacementCC, this.Engine.Fuel);
        }};
    """
    reduceLookUp = """
        function(Make, Model, DisplacementCC, Fuel){
            return Make, Model, DisplacementCC, Fuel;
    };
    """
    db.command('mapReduce', 'Owners', map=mapper1, reduce=reduceLookUp1, out={'reduce': 'joined'})
    db.command('mapReduce', 'Vehicles', map=mapper2, reduce=reduceLookUp1, out={'reduce': 'joined'})
    print(db["joined"].find({}))
# Main
def main():
    dataInsert()
    #print("Embedded output:")
    #getEmbedded()
    #print("\nAggregation output:")
    #aggregation()
    mapreduce("owner1")


if __name__ == "__main__":
    main()