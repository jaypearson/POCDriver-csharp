//package com.johnlpage.pocdriver;


//import java.util.*;

//import org.bson.BsonBinarySubType;
//import org.bson.BsonDocument;
//import org.bson.types.Binary;

//import de.svenjacobs.loremipsum.LoremIpsum;


using LoremNET;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;
//A Test Record is a MongoDB Record Object that is self populating

namespace POCDriver_csharp
{
    public class TestRecord
    {

        public BsonDocument internalDoc;
        private Random rng;
        private static List<List<Int32>> ar;

        private static BsonBinaryData blobData = null;

        private String CreateString(int length)
        { 
            //Console.Out.WriteLine("Generating sample data");
            var loremText = Lorem.Words(1000);
            //Console.Out.WriteLine("Done");

            var sb = new StringBuilder(loremText);
            
            //Double to size

            while (sb.Length < length)
            {
                //	Console.Out.WriteLine(" SB " + sb.Length() + " of " + length);
                sb.Append(sb.ToString());
            }

            //Trim to fit
            String rs = sb.ToString().Substring(0, length);
           
            return rs;
        }

        // This needs to be clever as we really need to be able to
        // Say - assuming nothing was removed - what is already in the DB
        // Therefore we will have a one-up per thread
        // A thread starting will find out what it's highest was

        private void AddOID(int workerid, int sequence)
        {
            BsonDocument oid = new BsonDocument("w", workerid).Add("i", sequence);
            internalDoc.Add("_id", oid);
        }

        // Just so we always know what the type of a given field is
        // Useful for querying, indexing etc

        private static int getFieldType(int fieldno)
        {
            if (fieldno == 0)
            {
                return 0; // Int
            }

            if (fieldno == 1)
            {
                return 2; // Date
            }

            if (fieldno == 3)
            {
                return 1; // Text
            }

            if (fieldno % 3 == 0)
            {
                return 0; // Int32
            }

            if (fieldno % 5 == 0)
            {
                return 2; // Date
            }

            return 1; // Text
        }

        public TestRecord(POCTestOptions testOpts) :
            this(testOpts.numFields, testOpts.depth, testOpts.textFieldLen,
                testOpts.workingset, 0, POCTestOptions.NUMBER_SIZE,
                new int[] { testOpts.arrays[0], testOpts.arrays[1] }, testOpts.blobSize)
        { }

        public TestRecord(int nFields, int depth, int stringLength, int workerID, int sequence, long numberSize, int[] array, int binsize)
        {
            internalDoc = new BsonDocument();
            rng = new Random();

            // Always a field 0
            AddOID(workerID, sequence);

            addFields(internalDoc, 0, nFields, depth, stringLength, numberSize);

            if (array[0] > 0)
            {
                if (ar == null)
                {
                    ar = new List<List<Int32>>(array[0]);
                    for (int q = 0; q < array[0]; q++)
                    {
                        List<Int32> sa = new List<Int32>(array[1]);
                        for (int w = 0; w < array[1]; w++)
                        {
                            sa.Add(0);
                        }
                        ar.Add(sa);
                    }
                }
                internalDoc.Add("arr", BsonArray.Create(ar));
            }
            if (blobData == null)
            {
                byte[] data = new byte[binsize * 1024];
                rng.NextBytes(data);
                blobData = new BsonBinaryData(data, BsonBinarySubType.Binary);
            }

            internalDoc.Add("bin", blobData);
        }

        /**
         * @param seq	 The sequence for this document as a whole
         * @param nFields The numbers of fields for this sub-document
         * @return the number of new fields added
         */
        private int addFields(BsonDocument doc, int seq, int nFields, int depth, int stringLength, long numberSize)
        {
            int fieldNo = seq;
            if (depth > 0)
            {
                // we need to create nodes not leaves
                int perLevel = Convert.ToInt32(Math.Pow(nFields, 1f / (depth + 1)));
                for (int i = 0; i < perLevel; i++)
                {
                    BsonDocument node = new BsonDocument();
                    doc.Add("node" + i, node);
                    fieldNo += addFields(node, fieldNo, nFields / perLevel, depth - 1, stringLength, numberSize);
                }
            }
            // fields
            while (fieldNo < nFields + seq)
            {
                int fType = getFieldType(fieldNo);
                if (fType == 0)
                {
                    // Field should always be a long this way

                    long r = Convert.ToInt64(Math.Abs(Math.Floor(rng.NextDouble()
                            * numberSize)));

                    doc.Add("fld" + fieldNo, r);
                }
                else if (fieldNo == 1 || fType == 2) // Field 2 is always a date
                                                     // as is every 5th
                {
                    // long r = (long) Math.Abs(Math.Floor(rng.nextGaussian() *
                    // Int64.MAX_VALUE));
                    DateTime now = DateTime.Now;
                    // Push it back 30 years or so
                    now.AddMilliseconds(-1 * Math.Abs(Math.Floor(rng.NextDouble() * 100000000 * 3000)));
                    doc.Add("fld" + fieldNo, now);
                }
                else
                {
                    // put in a string
                    String fieldContent = CreateString(stringLength);
                    doc.Add("fld" + fieldNo, fieldContent);
                }
                fieldNo++;
            }
            return fieldNo - seq;
        }

        public List<String> listFields()
        {
            List<String> fields = new List<String>();
            collectFields(internalDoc, "", fields);
            return fields;
        }

        private void collectFields(BsonDocument doc, String prefix, List<String> fields)
        {
            foreach (String key in doc.Names)
            {
                if (key.StartsWith("fld"))
                {
                    fields.Add(prefix + key);
                }
                else if (key.StartsWith("node"))
                {
                    // node
                    BsonDocument node = (BsonDocument)doc.GetValue(key);
                    collectFields(node, prefix + key + ".", fields);
                }
            }
        }

    }
}