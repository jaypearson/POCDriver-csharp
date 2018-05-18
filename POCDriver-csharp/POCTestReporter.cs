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
using System.Threading.Tasks;
using System.Threading;

namespace POCDriver_csharp
{
    public class POCTestReporter
    {
        private POCTestResults testResults;
        private MongoClient mongoClient;
        private POCTestOptions testOpts;
        private Logger logger;
        private Timer timer;

        public POCTestReporter(POCTestResults r, MongoClient mc, POCTestOptions t)
        {
            mongoClient = mc;
            testResults = r;
            testOpts = t;
            logger = LogManager.GetLogger("POCTestReporter");
        }

        private void logData(Object args)
        {
            timer.Change(testOpts.reportTime * 1000, Timeout.Infinite);
            Int64 insertsDone = testResults.GetOpsDone("inserts");
            logger.Info("------------------------");
            if (testOpts.sharded && !testOpts.singleserver)
            {
                var configdb = mongoClient.GetDatabase("config");
                var shards = configdb.GetCollection<BsonDocument>("shards");
                testOpts.numShards = (int)shards.Count(new BsonDocument());
            }

            logger.Info(string.Format(CultureInfo.CurrentUICulture,
                "After {0} seconds, {1:#,##0} new records inserted - collection has {2:#,##0} in total \n",
                    testResults.GetSecondsElapsed(), insertsDone, testResults.initialCount + insertsDone));

            var results = testResults.GetOpsPerSecondLastInterval();

            foreach (var o in POCTestResults.opTypes)
            {
                var begin = String.Format(CultureInfo.CurrentUICulture, "{0:#,##0} {1} per second since last report ", results[o], o);
                var end = "";
                var opsDone = testResults.GetOpsDone(o);
                if (opsDone > 0)
                {
                    var fastops = 100 - (testResults.GetSlowOps(o) * 100.0) / opsDone;
                    end = String.Format(CultureInfo.CurrentUICulture, "{0:0.##} % in under {1} milliseconds",
                        fastops, testOpts.slowThreshold);
                }
                else
                {
                    end = String.Format(CultureInfo.CurrentUICulture, "{0:0.##} % in under {1} milliseconds",
                        (float)100, testOpts.slowThreshold);
                }
                if (o == "rangequeries")
                    end = end + "\n";
                logger.Info($"{begin} {end}");
            }
        }

        public Task run()
        {
            timer = new Timer(logData, null, testOpts.reportTime * 1000, Timeout.Infinite);
            return Task.CompletedTask;
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
                "After {0} seconds, {1} new records inserted - collection has {2} in total \n",
                secondsElapsed, insertsDone, testResults.initialCount + insertsDone));

            foreach (var o in POCTestResults.opTypes)
            {
                Int64 opsDone = testResults.GetOpsDone(o);

                logger.Info(String.Format(CultureInfo.CurrentUICulture,
                    "{0} {1} per second on average", (int)(1f * opsDone / secondsElapsed), o));
            }
        }

        internal void Cancel()
        {
            timer.Dispose();
        }
    }
}