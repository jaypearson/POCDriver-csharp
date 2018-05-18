//package com.johnlpage.pocdriver;


//import com.mongodb.BasicDBObject;
//import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.IndexOptions;
//import org.bson.BsonDocument;

//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;

//import static com.mongodb.client.model.Filters.eq;

using MongoDB.Bson;
using MongoDB.Driver;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

//TODO - Change from System.println to a logging framework?
namespace POCDriver_csharp
{
    public class LoadRunner
    {
        private MongoClient mongoClient;
        private Logger logger;
        private bool isCancelled = false;

        private void PrepareSystem(POCTestOptions testOpts, POCTestResults results)
        {
            IMongoDatabase db;
            IMongoCollection<BsonDocument> coll;
            //Create indexes and suchlike
            db = mongoClient.GetDatabase(testOpts.namespaces[0]);
            coll = db.GetCollection<BsonDocument>(testOpts.namespaces[1]);
            if (testOpts.emptyFirst)
            {
                db.DropCollection(testOpts.namespaces[1]);
            }

            TestRecord testRecord = new TestRecord(testOpts);
            List<String> fields = testRecord.listFields();
            for (int x = 0; x < testOpts.secondaryidx; x++)
            {
                coll.Indexes.CreateOne(new BsonDocument(fields[x], 1));
            }
            if (testOpts.fulltext)
            {
                var options = new CreateIndexOptions();
                options.Background = true;
                var weights = new BsonDocument();
                weights.Add("lorem", 15);
                weights.Add("_fulltext.text", 5);
                options.Weights = weights;
                var index = new BsonDocument();
                index.Add("$**", "text");
                coll.Indexes.CreateOne(index, options);
            }

            results.initialCount = coll.Count(new BsonDocument());
            //Now have a look and see if we are sharded
            //And how many shards and make sure that the collection is sharded
            if (!testOpts.singleserver)
            {
                ConfigureSharding(testOpts);
            }
        }

        private void ConfigureSharding(POCTestOptions testOpts)
        {
            IMongoDatabase admindb = mongoClient.GetDatabase("admin");
            BsonDocument cr = admindb.RunCommand<BsonDocument>(new BsonDocument("serverStatus", 1));
            if (cr.GetValue("ok").AsDouble == 0)
            {
                logger.Info(cr.ToJson());
                return;
            }

            String procname = (String)cr.GetValue("process").AsString;
            if (procname != null && procname.Contains("mongos"))
            {
                testOpts.sharded = true;
                //Turn the auto balancer off - good code rarely needs it running constantly
                IMongoDatabase configdb = mongoClient.GetDatabase("config");
                IMongoCollection<BsonDocument> settings = configdb.GetCollection<BsonDocument>("settings");
                settings.UpdateOne(new BsonDocument("_id", "balancer"), new BsonDocument("$set", new BsonDocument("stopped", true)));
                //Console.Out.WriteLine("Balancer disabled");
                try
                {
                    //Console.Out.WriteLine("Enabling Sharding on Database");
                    admindb.RunCommand<BsonDocument>(new BsonDocument("enableSharding", testOpts.namespaces[0]));
                }
                catch (Exception e)
                {
                    if (!e.Message.Contains("already enabled"))
                        logger.Info(e.Message);
                }


                try
                {
                    //Console.Out.WriteLine("Sharding Collection");
                    admindb.RunCommand<BsonDocument>(new BsonDocument("shardCollection",
                            testOpts.namespaces[0] + "." + testOpts.namespaces[1]).Add("key", new BsonDocument("_id", 1)));
                }
                catch (Exception e)
                {
                    if (!e.Message.Contains("already"))
                        logger.Info(e.Message);
                }


                //See how many shards we have in the system - and get a list of their names
                //Console.Out.WriteLine("Counting Shards");
                IMongoCollection<BsonDocument> shards = configdb.GetCollection<BsonDocument>("shards");
                var shardc = shards.Find<BsonDocument>(new BsonDocument()).ToCursor();
                testOpts.numShards = 0;
                while (shardc.MoveNext())
                {
                    //Console.Out.WriteLine("Found a shard");
                    testOpts.numShards++;
                }

                //Console.Out.WriteLine("System has "+testOpts.numShards+" shards");
            }
        }

        public void RunLoad(POCTestOptions testOpts, POCTestResults testResults)
        {
            PrepareSystem(testOpts, testResults);
            // Report on progress by looking at testResults
            var reporter = new POCTestReporter(testResults, mongoClient, testOpts);

            try
            {                 
                // Using a thread pool we keep filled
                // +1 for the reporter thread the timer will use
                ThreadPool.SetMaxThreads(testOpts.numThreads+1, testOpts.numThreads+1);

                // Allow for multiple clients to run -
                // Check for testOpts.threadIdStart - this should be an Int32 to start
                // the 'workerID' for each set of threads.
                int threadIdStart = testOpts.threadIdStart;
                //Console.Out.WriteLine("threadIdStart="+threadIdStart);
                reporter.run();
                var workers = new List<MongoWorker>();
                var tasks = new List<Task>();
                for (int i = threadIdStart; i < (testOpts.numThreads + threadIdStart); i++)
                {
                    var worker = new MongoWorker(mongoClient, testOpts, testResults, i);
                    var task = new Task(new Action(() => worker.run(null)));
                    workers.Add(worker);
                    tasks.Add(task);
                    task.Start();
                }

                Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                    e.Cancel = true;
                    if (isCancelled)
                        return;
                    isCancelled = true;
                    foreach (var worker in workers)                 
                        worker.Cancel();
                    reporter.Cancel();
                    logger.Info("Cancellation requested - waiting on threads to finish");
                };

                Task.WaitAll(tasks.ToArray());
                //Console.Out.WriteLine("All Threads Complete: " + b);
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            finally
            {
                // do const report
                reporter.constReport();
            }
        }

        public LoadRunner(POCTestOptions testOpts)
        {
            logger = LogManager.GetLogger("LoadRunner");
            try
            {
                //For not authentication via connection string passing of user/pass only
                mongoClient = new MongoClient(new MongoUrl(testOpts.host));
            }
            catch (Exception e)
            {
                logger.Error(e.StackTrace);
            }
        }
    }
}