//package com.johnlpage.pocdriver;


//import com.mongodb.MongoClient;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import org.bson.BsonDocument;

//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;

using MongoDB.Bson;
using MongoDB.Driver;
using NLog;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace POCDriver_csharp
{
    public class POCTestReporter
    {
        private POCTestResults testResults;
        private MongoClient mongoClient;
        private POCTestOptions testOpts;
        private Logger logger;

        public POCTestReporter(POCTestResults r, MongoClient mc, POCTestOptions t)
        {
            mongoClient = mc;
            testResults = r;
            testOpts = t;
            logger = LogManager.GetLogger("POCTestReporter");
        }

        private void logData()
        { 

            Int64 insertsDone = testResults.GetOpsDone("inserts");
            if (testResults.GetSecondsElapsed() < testOpts.reportTime)
                return;
            logger.Info("------------------------");
            if (testOpts.sharded && !testOpts.singleserver)
            {
                IMongoDatabase configdb = mongoClient.GetDatabase("config");
                IMongoCollection<BsonDocument> shards = configdb.GetCollection<BsonDocument>("shards");
                testOpts.numShards = (int)shards.Count(new BsonDocument());
            }
            DateTime todaysdate = DateTime.Now;
            logger.Info(string.Format("After %d seconds (%s), %,d new records inserted - collection has %,d in total \n",
                    testResults.GetSecondsElapsed(), todaysdate.ToShortTimeString(), insertsDone, testResults.initialCount + insertsDone));

            Dictionary<String, Int64> results = testResults.GetOpsPerSecondLastInterval();
            String[] opTypes = POCTestResults.opTypes;

            foreach (var o in opTypes)
            {
                logger.Info(String.Format(CultureInfo.CurrentUICulture, "{0:#,#,,} {1} per second since last report ", results[o], o));

                logger.Info(String.Format(CultureInfo.CurrentUICulture, "{0},{1:#,#,,},{2:#,#,,}",
                    todaysdate, testResults.GetSecondsElapsed(), insertsDone));

                Int64 opsDone = testResults.GetOpsDone(o);
                if (opsDone > 0)
                {
                    Double fastops = 100 - (testResults.GetSlowOps(o) * 100.0)
                            / opsDone;
                    logger.Info(String.Format(CultureInfo.CurrentUICulture, "{0:0.##} % in under {1:#,#,,} milliseconds",
                        fastops, testOpts.slowThreshold));
                }
                else
                {
                    logger.Info(String.Format(CultureInfo.CurrentUICulture, "{0:0.##} % in under {1:#,#,,} milliseconds",
                        (float)100, testOpts.slowThreshold));
                }
            }
        }

        public void run(Object arg)
        {
            logData();
        }

        /**
         * Output a const summary
         */
        public void constReport()
        {
            Int64 insertsDone = testResults.GetOpsDone("inserts");

            Int64 secondsElapsed = testResults.GetSecondsElapsed();

            logger.Info("------------------------");
            logger.Info(String.Format(CultureInfo.CurrentUICulture,
                "After {0:#,#,,} seconds, {1:#,#,,} new records inserted - collection has {2:#,#,,} in total \n",
                secondsElapsed, insertsDone, testResults.initialCount + insertsDone));

            foreach (var o in POCTestResults.opTypes)
            {
                Int64 opsDone = testResults.GetOpsDone(o);

                logger.Info(String.Format(CultureInfo.CurrentUICulture,
                    "{0:#,#,,} {1} per second on average", (int)(1f * opsDone / secondsElapsed), o));
            }
        }
    }
}