using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleMongo
{
    class Program
    {
        static IMongoCollection<BsonDocument> productsBson = null;
        static IMongoCollection<Product> productsModel = null;
        static IMongoCollection<BsonDocument> indexTest = null;

        static IMongoDatabase db;

        static void Main(string[] args)
        {
      
            var mongoClient = new MongoClient();
            db = mongoClient.GetDatabase("test");


            if (!CollectionExistsAsync("Products").Result)
                db.CreateCollection("Products");

            if (!CollectionExistsAsync("indexTest").Result)
                db.CreateCollection("indexTest");

            productsBson = db.GetCollection<BsonDocument>("Products");

            productsModel = db.GetCollection<Product>("Products");

            indexTest = db.GetCollection<BsonDocument>("indexTest");
           // RemoveCollection();
           // InsertBsonProduct();
           //InsertProductEntity();

            GetAllBsons();

            //GetProductByName("Iphone 5S");

            //CalculatePriceGroupedByName();

            //UpdateIphone5To5G();

            //*******  Mongo Index testing******
            //
            //InsertManyItemsToNewCollection();
            Console.WriteLine("No Index");
            FindUser();

            CreateIndex();
            Console.WriteLine("With Index");

            FindUser();

            dropIndex();
            Console.WriteLine("No Index");

            FindUser();
            Console.ReadLine();

        }
        public static async Task<bool> CollectionExistsAsync(string collectionName)
        {
            var filter = new BsonDocument("name", collectionName);
            //filter by collection name
            var collections = await db.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            //check for existence
            return await collections.AnyAsync();
        }

        static void InsertBsonProduct()
        {
            BsonDocument doc = new BsonDocument
            {
                {"Name","Iphone 10"},
                {"Count",20 },
                {"Price",500 },

                {"Manufacturer",new BsonDocument{{ "Country", "China" },{ "Address", "China streat 1" }  } },
                {"Details",new BsonArray{"2018 year","Sceen Size 8'","IOS 13 Version"} }
            };
            BsonDocument doc2 = new BsonDocument
            {
               {"Name","Iphone 12S"},
                {"Count",12 },
                {"Price",1500 },

                {"Manufacturer",new BsonDocument{{ "Country", "California" },{ "Address", "China streat 1" }  } },
                {"Details",new BsonArray{"2020 year","Sceen Size 8.7'","IOS 13.5 Version"} }
            };
            productsBson.InsertMany(new BsonDocument[] {doc,doc2 });
        }
        static void UpdateIphone5To5G()
        {
            var filter=Builders<Product>.Filter.Eq("Details", "4G");
            var update= Builders<Product>.Update.Set("Details", "5G");
            var result=productsModel.UpdateOne(filter, update);

           

        

        }
        static void InsertProductEntity()
        {
            var prod = new Product
            {
                Name = "Iphone 5S",
                Count = 8,
                Price = 100,
                Details = new List<string> { "5 mpx", "4'", "4G" },
                Manufacturer = new Manufacturer
                {
                    Address = "street 5",
                    Country = "China"
                }
            };

            productsModel.InsertOne(prod);
        }

        static void GetProductByName(string name)
        {
           var filter= Builders<Product>.Filter.Eq("Name", name);
           var result= productsModel.Find<Product>(string.IsNullOrEmpty(name)?Builders<Product>.Filter.Empty: filter).ToList();
            //problem with deserialization
        }
        static void CalculatePriceGroupedByName()
        {

            var res=productsModel.Aggregate().Group(
                x => x.Name,
                group => new
                {
                    Name = group.Key,
                    Total = group.Sum(x => x.Price*x.Count)
                }
                ).ToList();

            foreach(var item in res)
            {
                Console.WriteLine(item.Name+"\t"+item.Total);
            }
        }
        static void GetAllBsons()
        {
            var filter = Builders<BsonDocument>.Filter.Empty;
            var result= productsBson.Find(filter).ToList();
            Console.WriteLine(new string('b', 18));
            foreach (var item in result)
                Console.WriteLine(item.ToJson());
            Console.WriteLine(new string('b', 18));


        }
        static void RemoveCollection()
        {   
            db.DropCollection("Products");
            //Or
           // productsModel.DeleteMany<Product>(x => x._id != null);
        }

        static void InsertManyItemsToNewCollection()
        {
            //Realy Big Data ;-)
            for (int i = 0; i < 1000000; i++)
            {
                BsonDocument doc = new BsonDocument
                {
                    {"Name",$"user {i}"},

                };
                indexTest.InsertOne(doc);
            }
            
        }
        static void FindUser()
        {
            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            var users = indexTest.Find<BsonDocument>(Builders<BsonDocument>.Filter.Eq("Name", "user 1")).ToList();
            stopwatch.Stop();
            Console.WriteLine(new string('*', 18));
            Console.WriteLine($"First user was find in {stopwatch.ElapsedMilliseconds}");
            foreach (var item in users)
                Console.WriteLine(item.ToJson());

            stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            users = indexTest.Find<BsonDocument>(Builders<BsonDocument>.Filter.Eq("Name", "user 999999")).ToList();
            stopwatch.Stop();
            Console.WriteLine(new string('*', 18));
            Console.WriteLine($"last user was find in {stopwatch.ElapsedMilliseconds}");
            foreach (var item in users)
                Console.WriteLine(item.ToJson());
        }
        static void CreateIndex()
        {
            IndexKeysDefinition<BsonDocument> keys = "{ Name: 1 }";
            var indexModel = new CreateIndexModel<BsonDocument>(keys);
            indexTest.Indexes.CreateOne(indexModel);
        }
        static void dropIndex()
        {
           
            indexTest.Indexes.DropOne("Name"); 
        }
 
    }
  
    class Product
    {
        public ObjectId _id { get; set; }
        public string Name { get; set; }
        public int Count { get; set; }
        public int Price { get; set; }
        public Manufacturer Manufacturer { get; set; }
    
      

        public List<string> Details { get; set; }

       
    }

    class Manufacturer
    {
        [BsonElement("Country")]
        public string Country { get; set; }
        [BsonElement("Address")]
        public string Address { get; set; }
    }


   
    
}
