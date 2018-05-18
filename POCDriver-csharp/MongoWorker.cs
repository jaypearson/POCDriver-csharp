//package com.johnlpage.pocdriver;

//import com.mongodb.MongoClient;
//import com.mongodb.bulk.BulkWriteResult;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.InsertOneModel;
//import com.mongodb.client.model.UpdateManyModel;
//import com.mongodb.client.model.WriteModel;
//import org.apache.commons.math3.distribution.ZipfDistribution;
//import org.bson.BsonDocument;

//import java.util.ArrayList;
//import java.util.Date;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Random;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

//import static com.mongodb.client.model.Projections.fields;
//import static com.mongodb.client.model.Projections.include;
//import static com.mongodb.client.model.Sorts.descending;

using MathNet.Numerics.Distributions;
using MongoDB.Bson;
using MongoDB.Driver;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading;

namespace POCDriver_csharp
{
    public class MongoWorker
    {
        private Logger logger;
        private MongoClient mongoClient;
        private IMongoCollection<BsonDocument> coll;
        private IList<IMongoCollection<BsonDocument>> colls;
        private POCTestOptions testOpts;
        private POCTestResults testResults;
        private int workerID;
        private int sequence;
        private int numShards = 0;
        private Random rng;
        private Zipf zipf;
        private Boolean workflowed = false;
        private Boolean zipfian = false;
        private String workflow;
        private int workflowStep = 0;
        private IList<BsonDocument> keyStack;
        private int lastCollection;
        private int maxCollections;
        private bool isCancelled = false;

        private void ReviewShards()
        {
            //Console.Out.WriteLine("Reviewing chunk distribution");
            if (testOpts.sharded && !testOpts.singleserver)
            {
                // I'd like to pick a shard and write there - it's going to be
                // faster and
                // We can ensure we distribute our workers over out shards
                // So we will tell mongo that's where we want our records to go
                //Console.Out.WriteLine("Sharded and not a single server");
                var admindb = mongoClient.GetDatabase("admin");
                Boolean split = false;

                while (!split)
                {

                    try
                    {
                        //		Console.Out.WriteLine("Splitting a chunk");
                        admindb.RunCommand<BsonDocument>(new BsonDocument("split",
                                testOpts.namespaces[0] + "." + testOpts.namespaces[1])
                                .Add("middle",
                                        new BsonDocument("_id", new BsonDocument("w",
                                                workerID).Add("i", sequence + 1))));
                        split = true;
                    }
                    catch (Exception e)
                    {

                        if (e.Message.Contains("is a boundary key of existing"))
                        {
                            split = true;
                        }
                        else
                        {
                            if (!e.Message.Contains("could not aquire collection lock"))
                                Console.Out.WriteLine(e.Message);
                            try
                            {
                                Thread.Sleep(1000);
                            }
                            catch (Exception ignored)
                            {
                            }
                        }
                    }

                }
                // And move that to a shard - which shard? take my workerid and mod
                // it with the number of shards
                int shardno = workerID % testOpts.numShards;
                // Get the name of the shard

                var shardlist = mongoClient.GetDatabase("config")
                        .GetCollection<BsonDocument>("shards")
                        .Find(new BsonDocument())
                        .Skip(shardno)
                        .Limit(1)
                        .ToCursor();
                //Console.Out.WriteLine("Getting shard name");
                String shardName = "";
                while (shardlist.MoveNext())
                {
                    foreach (var obj in shardlist.Current)
                    {
                        shardName = obj.GetValue("_id").AsString;
                        //Console.Out.WriteLine(shardName);
                    }
                }

                Boolean move = false;
                while (!move)
                {
                    try
                    {
                        admindb.RunCommand<BsonDocument>(new BsonDocument("moveChunk",
                                testOpts.namespaces[0] + "." + testOpts.namespaces[1])
                                .Add("find",
                                        new BsonDocument("_id", new BsonDocument("w",
                                                workerID).Add("i", sequence + 1)))
                                .Add("to", shardName));
                        move = true;
                    }
                    catch (Exception e)
                    {
                        Console.Out.WriteLine(e.Message);
                        if (e.Message.Contains("that chunk is already on that shard"))
                        {
                            move = true;
                        }
                        else
                        {
                            if (!e.Message.Contains("could not aquire collection lock"))
                                Console.Out.WriteLine(e.Message);
                            try
                            {
                                Thread.Sleep(1000);
                            }
                            catch (Exception ignored)
                            {
                            }
                        }
                    }


                }

                //Console.Out.WriteLine("Moved {w:" + workerID + ",i:" + (sequence + 1)
                //		+ "} to " + shardName);
                numShards = testOpts.numShards;
            }
        }

        public MongoWorker(MongoClient c, POCTestOptions t, POCTestResults r, int id)
        {
            logger = LogManager.GetLogger($"MongoWorker {id}");
            mongoClient = c;

            //Ping
            c.GetDatabase("admin").RunCommand<BsonDocument>(new BsonDocument("ping", 1));
            testOpts = t;
            testResults = r;
            workerID = id;
            var db = mongoClient.GetDatabase(testOpts.namespaces[0]);
            maxCollections = testOpts.numcollections;
            String baseCollectionName = testOpts.namespaces[1];
            if (maxCollections > 1)
            {
                colls = new List<IMongoCollection<BsonDocument>>();
                lastCollection = 0;
                for (int i = 0; i < maxCollections; i++)
                {
                    String str = baseCollectionName + i;
                    colls.Add(db.GetCollection<BsonDocument>(str));
                }
            }
            else
            {
                coll = db.GetCollection<BsonDocument>(baseCollectionName);
            }

            // id
            sequence = getHighestID();

            ReviewShards();
            rng = new Random();
            if (testOpts.zipfsize > 0)
            {
                zipfian = true;
                zipf = new Zipf(0.99, testOpts.zipfsize);
            }

            if (!string.IsNullOrWhiteSpace(testOpts.workflow))
            {
                workflow = testOpts.workflow;
                workflowed = true;
                keyStack = new List<BsonDocument>();
            }

        }

        private int getNextVal(int mult)
        {
            int rval;
            if (zipfian)
            {
                rval = zipf.Sample();
            }
            else
            {
                rval = (int)Math.Abs(Math.Floor(rng.NextDouble() * mult));
            }
            return rval;
        }

        private int getHighestID()
        {
            int rval = 0;

            rotateCollection();
            BsonDocument query = new BsonDocument();

            //TODO Refactor the query for 3.0 driver
            BsonDocument limits = new BsonDocument("$gt", new BsonDocument("w", workerID));
            limits.Add("$lt", new BsonDocument("w", workerID + 1));

            query.Add("_id", limits);

            var projection = new ProjectionDefinitionBuilder<BsonDocument>()
                .Include("_id");
            var sort = new SortDefinitionBuilder<BsonDocument>()
                .Descending("_id");
            BsonDocument myDoc = coll.Find(query)
                .Project(projection)
                .Sort(sort)
                .FirstOrDefault();
            if (myDoc != null)
            {
                BsonDocument id = (BsonDocument)myDoc.GetValue("_id");
                rval = id.GetValue("i").AsInt32 + 1;
            }
            return rval;
        }

        //This one was a major rewrite as the whole Bulk Ops API changed in 3.0

        private void flushBulkOps(List<WriteModel<BsonDocument>> bulkWriter)
        {
            // Time this.
            rotateCollection();
            DateTime starttime = DateTime.Now;

            //This is where ALL writes are happening
            //So this can fail part way through if we have a failover
            //In which case we resubmit it

            Boolean submitted = false;
            BulkWriteResult bwResult = null;

            while (!submitted && bulkWriter.Count != 0)
            {  // can be empty if we removed a Dupe key error
                try
                {
                    submitted = true;
                    bwResult = coll.BulkWrite(bulkWriter);
                }
                catch (Exception e)
                {
                    //We had a problem with this bulk op - some may be completed, some may not

                    //I need to resubmit it here
                    String error = e.Message;

                    //Check if it's a sup key and remove it
                    var p = BsonRegularExpression.Create("dup key: \\{ : \\{ w: (.*?), i: (.*?) }");
                    //	Pattern p = Pattern.compile("dup key");

                    var m = p.AsRegex.Match(error);
                    if (m.Success)
                    {
                        //Console.Out.WriteLine("Duplicate Key");
                        //int thread = Int32.Parse(m.group(1));
                        int uniqid = Int32.Parse(m.Groups[2].Value);
                        //Console.Out.WriteLine(" ID = " + thread + " " + uniqid );
                        Boolean found = false;
                        foreach (var entry in bulkWriter.ToArray())
                        {
                            //Check if it's a InsertOneModel

                            var insertOneModel = entry as InsertOneModel<BsonDocument>;
                            if (insertOneModel != null)
                            {
                                var id = (BsonDocument)insertOneModel.ToBsonDocument().GetValue("_id");

                                //int opthread=id.getInt32("w");
                                //int opid = id.getInt32("i");
                                //Console.Out.WriteLine("opthread: " + opthread + "=" + thread + " opid: " + opid + "=" + uniqid);
                                if (id.GetValue("i").AsInt32 == uniqid)
                                {
                                    //Console.Out.WriteLine(" Removing " + thread + " " + uniqid + " from bulkop as already inserted");
                                    bulkWriter.Remove(entry);
                                    found = true;
                                }
                            }
                        }
                        if (!found)
                        {
                           logger.Warn("Cannot find failed op in batch!");
                        }
                    }
                    else
                    {
                        // Some other error occurred - possibly MongoCommandException, MongoTimeoutException
                        logger.Error(e.GetType().Name + ": " + error);
                        // Print a full stacktrace since we're in debug mode
                        if (testOpts.debug)
                            logger.Debug(e.StackTrace);
                    }
                    //Console.Out.WriteLine("No result returned");
                    submitted = false;
                }
            }

            var taken = (DateTime.Now - starttime).TotalMilliseconds;

            var icount = bwResult.InsertedCount;
            var ucount = bwResult.MatchedCount;

            // If the bulk op is slow - ALL those ops were slow

            if (taken > testOpts.slowThreshold)
            {
                testResults.RecordSlowOp("inserts", icount);
                testResults.RecordSlowOp("updates", ucount);
            }
            testResults.RecordOpsDone("inserts", icount);
        }

        private BsonDocument simpleKeyQuery()
        {
            // Key Query
            rotateCollection();
            BsonDocument query = new BsonDocument();
            int range = sequence * testOpts.workingset / 100;
            int rest = sequence - range;

            int recordno = rest + getNextVal(range);

            query.Add("_id",
                    new BsonDocument("w", workerID).Add("i", recordno));
            DateTime starttime = DateTime.Now;
            BsonDocument myDoc;
            List<String> projFields = new List<String>(testOpts.numFields);

            if (testOpts.projectFields == 0)
            {
                myDoc = coll.Find(query).FirstOrDefault();
            }
            else
            {
                int numProjFields = (testOpts.projectFields <= testOpts.numFields) ? testOpts.projectFields : testOpts.numFields;
                int i = 0;
                while (i < numProjFields)
                {
                    projFields.Add("fld" + i);
                    i++;
                }
                var projection = new ProjectionDefinitionBuilder<BsonDocument>();
                foreach (var field in projFields)
                    projection.Include(field);
                myDoc = coll.Find(query)
                    .Project<BsonDocument>(projection.Combine())
                    .FirstOrDefault();
            }

            if (myDoc != null)
            {
                var taken = (DateTime.Now - starttime).TotalMilliseconds;
                if (taken > testOpts.slowThreshold)
                {
                    testResults.RecordSlowOp("keyqueries", 1);
                }
                testResults.RecordOpsDone("keyqueries", 1);
            }
            return myDoc;
        }

        private void rangeQuery()
        {
            // Key Query
            rotateCollection();
            BsonDocument query = new BsonDocument();
            var projection = new ProjectionDefinitionBuilder<BsonDocument>();
            int recordno = getNextVal(sequence);
            query.Add("_id", new BsonDocument("$gt", new BsonDocument("w",
                    workerID).Add("i", recordno)));
            DateTime starttime = new DateTime();
            IAsyncCursor<BsonDocument> cursor;
            if (testOpts.projectFields == 0)
            {
                cursor = coll.Find(query)
                    .Limit(testOpts.rangeDocs)
                    .ToCursor();
            }
            else
            {
                int numProjFields = (testOpts.projectFields <= testOpts.numFields) ? testOpts.projectFields : testOpts.numFields;
                for (int i = 0; i < numProjFields; i++)
                { 
                    projection.Include("fld" + i);
                }
                cursor = coll.Find(query)
                    .Project<BsonDocument>(projection.Combine())
                    .Limit(testOpts.rangeDocs)
                    .ToCursor();
            }
            while (cursor.MoveNext())
            {
                var batch = cursor.Current;
                foreach (var doc in batch)
                {
                    // Consume the cursor
                }
            }
            cursor.Dispose();
            
            var taken = (DateTime.Now - starttime).TotalMilliseconds;
            if (taken > testOpts.slowThreshold)
            {
                testResults.RecordSlowOp("rangequeries", 1);
            }
            testResults.RecordOpsDone("rangequeries", 1);
        }

        private void rotateCollection()
        {
            if (maxCollections > 1)
            {
                coll = colls[lastCollection];
                lastCollection = (lastCollection + 1) % maxCollections;
            }
        }

        private void updateSingleRecord(List<WriteModel<BsonDocument>> bulkWriter)
        {
            updateSingleRecord(bulkWriter, null);
        }

        private void updateSingleRecord(List<WriteModel<BsonDocument>> bulkWriter,BsonDocument key)
        {
            // Key Query
            rotateCollection();
            BsonDocument query = new BsonDocument();
            BsonDocument change;

            if (key == null)
            {
                int range = sequence * testOpts.workingset / 100;
                int rest = sequence - range;

                int recordno = rest + getNextVal(range);

                query.Add("_id", new BsonDocument("w", workerID).Add("i", recordno));
            }
            else
            {
                query.Add("_id", key);
            }

            int updateFields = (testOpts.updateFields <= testOpts.numFields) ? testOpts.updateFields : testOpts.numFields;

            if (updateFields == 1)
            {
                long changedfield = getNextVal((int)POCTestOptions.NUMBER_SIZE);
                BsonDocument fields = new BsonDocument("fld0", changedfield);
                change = new BsonDocument("$set", fields);
            }
            else
            {
                TestRecord tr = createNewRecord();
                tr.internalDoc.Remove("_id");
                change = new BsonDocument("$set", tr.internalDoc);
            }

            if (!testOpts.findandmodify)
            {
                bulkWriter. Add(new UpdateManyModel<BsonDocument>(query, change));
            }
            else
            {
                coll.FindOneAndUpdate(query, change); //These are immediate not batches
            }
            testResults.RecordOpsDone("updates", 1);
        }

        private TestRecord createNewRecord()
        {
            int[] arr = new int[2];
            arr[0] = testOpts.arrays[0];
            arr[1] = testOpts.arrays[1];
            return new TestRecord(testOpts.numFields, testOpts.depth, testOpts.textFieldLen,
                    workerID, sequence++, POCTestOptions.NUMBER_SIZE,
                    arr, testOpts.blobSize);
        }

        private TestRecord insertNewRecord(List<WriteModel<BsonDocument>> bulkWriter)
        {
            TestRecord tr = createNewRecord();
            bulkWriter.Add(new InsertOneModel<BsonDocument>(tr.internalDoc));
            return tr;
        }

        public void Cancel()
        {
            isCancelled = true;
            logger.Info("Cancellation requested...");
        }

        public void run(Object arg)
        {
            // Use a bulk inserter - even if ony for one
            List<WriteModel<BsonDocument>> bulkWriter;

            try
            {
                bulkWriter = new List<WriteModel<BsonDocument>>();
                int bulkops = 0;

                int c = 0;
                logger.Info("Worker thread " + workerID + " Started.");
                while (testResults.GetSecondsElapsed() < testOpts.duration)
                {
                    if (isCancelled)
                        break ;
                    c++;
                    //Timer isn't granullar enough to sleep for each
                    if (testOpts.opsPerSecond > 0)
                    {
                        double threads = testOpts.numThreads;
                        double opsperthreadsecond = testOpts.opsPerSecond / threads;
                        double sleeptimems = 1000 / opsperthreadsecond;

                        if (c == 1)
                        {
                            //First time randomise

                            Random r = new Random();
                            sleeptimems = r.Next((int)Math.Floor(sleeptimems));

                        }
                        Thread.Sleep((int)Math.Floor(sleeptimems));
                    }
                    if (!workflowed)
                    {
                        // Console.Out.WriteLine("Random op");
                        // Choose the type of op
                        int allops = testOpts.insertops + testOpts.keyqueries
                                + testOpts.updates + testOpts.rangequeries
                                + testOpts.arrayupdates;
                        int randop = getNextVal(allops);

                        if (randop < testOpts.insertops)
                        {
                            insertNewRecord(bulkWriter);
                            bulkops++;
                        }
                        else if (randop < testOpts.insertops
                              + testOpts.keyqueries)
                        {
                            simpleKeyQuery();
                        }
                        else if (randop < testOpts.insertops
                              + testOpts.keyqueries + testOpts.rangequeries)
                        {
                            rangeQuery();
                        }
                        else
                        {
                            // An in place single field update
                            // fld 0 - set to random number
                            updateSingleRecord(bulkWriter);
                            if (!testOpts.findandmodify)
                                bulkops++;
                        }
                    }
                    else
                    {
                        // Following a preset workflow
                        String wfop = workflow.Substring(workflowStep,
                                workflowStep + 1);

                        // Console.Out.WriteLine("Executing workflow op [" + workflow +
                        // "] " + wfop);
                        if (wfop.Equals("i"))
                        {
                            // Insert a new record, push it's key onto our stack
                            TestRecord r = insertNewRecord(bulkWriter);
                            keyStack.Add((BsonDocument)r.internalDoc.GetValue("_id"));
                            bulkops++;
                            // Console.Out.WriteLine("Insert");
                        }
                        else if (wfop.Equals("u"))
                        {
                            if (keyStack.Count > 0)
                            {
                                updateSingleRecord(bulkWriter, keyStack[keyStack.Count - 1]);
                                // Console.Out.WriteLine("Update");
                                if (!testOpts.findandmodify)
                                    bulkops++;
                            }
                        }
                        else if (wfop.Equals("p"))
                        {
                            // Pop the top thing off the stack
                            if (keyStack.Count > 0)
                            {
                                keyStack.RemoveAt(keyStack.Count - 1);
                            }
                        }
                        else if (wfop.Equals("k"))
                        {
                            // Find a new record an put it on the stack
                            BsonDocument r = simpleKeyQuery();
                            if (r != null)
                            {
                                keyStack.Add((BsonDocument)r.GetValue("_id"));
                            }
                        }

                        // If we have reached the end of the wfops then reset
                        workflowStep++;
                        if (workflowStep >= workflow.Length)
                        {
                            workflowStep = 0;
                            keyStack = new List<BsonDocument>();
                        }
                    }

                    if (c % testOpts.batchSize == 0)
                    {
                        if (bulkops > 0)
                        {
                            flushBulkOps(bulkWriter);
                            bulkWriter.Clear();
                            bulkops = 0;
                            // Check and see if we need to rejig sharding
                            if (numShards != testOpts.numShards)
                            {
                                ReviewShards();
                            }
                        }
                    }

                }
            }
            catch (Exception e)
            {
                logger.Error("Error: " + e.Message);
                if (testOpts.debug)
                    logger.Debug(e.StackTrace);
            }
        }
    }
}
