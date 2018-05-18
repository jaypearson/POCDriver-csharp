
//package com.johnlpage.pocdriver;


//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonParser;
//import org.apache.commons.cli.ParseException;
//import org.bson.BsonBinaryWriter;
//import org.bson.codecs.BsonDocumentCodec;
//import org.bson.codecs.EncoderContext;
//import org.bson.io.BasicOutputBuffer;

//import java.util.logging.LogManager;

using MongoDB.Bson;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Text;
using CommandLine;

namespace POCDriver_csharp
{
    public class POCDriver
    {
        public static Logger logger;

        public static void Main(String[] args)
        {
            var result = Parser.Default.ParseArguments<POCTestOptions>(args)
                .WithNotParsed(errors =>
                {
                    foreach (var error in errors)
                    {
                        switch (error.Tag)
                        {
                            case ErrorType.HelpRequestedError:
                            case ErrorType.HelpVerbRequestedError:
                            case ErrorType.VersionRequestedError:
                                break;
                            default:
                                Console.Error.WriteLine(error);
                                break;
                        }
                    }
                })
                .WithParsed(testOpts =>
                {
                    ConfigureLogging(testOpts);
                    try
                    {
                        logger = LogManager.GetLogger("POCDriver");
                        logger.Info("MongoDB Proof Of Concept - Load Generator");

                        if (testOpts.arrayupdates > 0 && (testOpts.arrays[0] < 1 || testOpts.arrays[1] < 1))
                        {
                            logger.Info("You must specify an array size to update arrays");
                            return;
                        }

                        if (testOpts.printOnly)
                        {
                            printTestBsonDocument(testOpts);
                            return;
                        }

                        var testResults = new POCTestResults();
                        var runner = new LoadRunner(testOpts);
                        runner.RunLoad(testOpts, testResults);
                    }
                    catch (Exception e)
                    {
                        logger.Error(e.Message);
                        return;
                    }
                });
        }

        private static void ConfigureLogging(POCTestOptions testOpts)
        {
            // Step 1. Create configuration object 
            var config = new LoggingConfiguration();

            // Step 2. Create targets and add them to the configuration 
            var consoleTarget = new ColoredConsoleTarget();
            config.AddTarget("console", consoleTarget);

            var fileTarget = new FileTarget();
            config.AddTarget("file", fileTarget);

            // Step 3. Set target properties 
            consoleTarget.Layout = @"${date:format=HH\:mm\:ss} ${logger} ${message}";
            if (string.IsNullOrWhiteSpace(testOpts.logfile))
                fileTarget.FileName = "${basedir}/POCDriver-csharp_log.txt";
            else
                fileTarget.FileName = testOpts.logfile;
            fileTarget.Layout = @"${date:format=HH\:mm\:ss} ${logger} ${message}";

            // Step 4. Define rules
            var rule1 = new LoggingRule("*", testOpts.debug ? LogLevel.Debug : LogLevel.Info, consoleTarget);
            config.LoggingRules.Add(rule1);

            var rule2 = new LoggingRule("*", testOpts.debug ? LogLevel.Debug : LogLevel.Info, fileTarget);
            config.LoggingRules.Add(rule2);

            // Step 5. Activate the configuration
            LogManager.Configuration = config;
        }

        private static void printTestBsonDocument(POCTestOptions testOpts)
        {
            //Sets up sample data don't remove
            int[] arr = new int[2];
            arr[0] = testOpts.arrays[0];
            arr[1] = testOpts.arrays[1];
            var tr = new TestRecord(testOpts.numFields, testOpts.depth, testOpts.textFieldLen,
                    1, 12345678, POCTestOptions.NUMBER_SIZE, arr, testOpts.blobSize);
            //Console.Out.WriteLine(tr);

            String json = tr.internalDoc.ToJson();
            StringBuilder newJson = new StringBuilder();
            int arrays = 0;

            // Collapse inner newlines
            Boolean inquotes = false;
            for (int c = 0; c < json.Length; c++)
            {
                char inChar = json[c];
                if (inChar == '[')
                {
                    arrays++;
                }
                if (inChar == ']')
                {
                    arrays--;
                }
                if (inChar == '"')
                {
                    inquotes = !inquotes;
                }

                if (arrays > 1 && inChar == '\n')
                {
                    continue;
                }
                if (arrays > 1 && !inquotes && inChar == ' ')
                {
                    continue;
                }
                newJson.Append(json[c]);
            }
            logger.Info(newJson.ToString());

            byte[] bsonBytes = tr.internalDoc.ToBson();
            long length = bsonBytes.LongLength;
            logger.Info(String.Format("Records are {0:0.##} KB each as BSON", (float)length / 1024));
        }
    }
}
