//package com.johnlpage.pocdriver;


//import java.util.Date;
//import java.util.HashMap;
//import java.util.concurrent.ConcurrentHashMap;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace POCDriver_csharp
{
    public class POCTestResults {

        /**
         * The time this LoadRunner started
         */
        private DateTime startTime;
        private DateTime lastIntervalTime;
        public long initialCount;

        public static String[] opTypes = { "inserts", "keyqueries", "updates", "rangequeries" };
        private ConcurrentDictionary<String, POCopStats> opStats;


        public POCTestResults() {
            startTime = DateTime.Now;
            lastIntervalTime = new DateTime();
            opStats = new ConcurrentDictionary<String, POCopStats>();

            foreach (String s in opTypes) {
                var newValue = new POCopStats();
                opStats.AddOrUpdate(s, newValue, (key, oldValue) => newValue);
            }
        }

        //This returns inserts per second since we last called it
        //Rather than us keeping an overall figure

        public Dictionary<String, Int64> GetOpsPerSecondLastInterval() {

            Dictionary<String, Int64> rval = new Dictionary<String, Int64>();

            DateTime now = DateTime.Now;
            double milliSecondsSinceLastCheck = (now - lastIntervalTime).TotalMilliseconds;

            foreach (String s in opTypes) {
                Int64 opsNow = GetOpsDone(s);
                Int64 opsPrev = GetPrevOpsDone(s);
                Int64 opsPerInterval = Convert.ToInt64(((opsNow - opsPrev) * 1000) / milliSecondsSinceLastCheck);
                rval.Add(s, opsPerInterval);
                SetPrevOpsDone(s, opsNow);
            }

            lastIntervalTime = now;

            return rval;
        }

        public Int64 GetSecondsElapsed() {
            return Convert.ToInt64((DateTime.Now - startTime).TotalSeconds);
        }


        private Int64 GetPrevOpsDone(String opType) {
            POCopStats os;
            if (!opStats.TryGetValue(opType, out os))
            {
                Console.Out.WriteLine("Cannot fetch opstats for " + opType);
                return 0;
            }
            else
            {
                return os.intervalCount.Value;
            }
        }

        private void SetPrevOpsDone(String opType, Int64 numOps) {
            POCopStats os;
            if (!opStats.TryGetValue(opType, out os))
            {
                Console.Out.WriteLine("Cannot fetch opstats for " + opType);
            }
            else
            {
                os.intervalCount.Exchange(numOps);
            }
        }

        public Int64 GetOpsDone(String opType) {
            POCopStats os;
            if (!opStats.TryGetValue(opType, out os))
            {
                Console.Out.WriteLine("Cannot fetch opstats for " + opType);
                return 0;
            }
            else
            {
                return os.totalOpsDone.Value;
            }
        }


        public Int64 GetSlowOps(String opType) {
            POCopStats os;
            if (!opStats.TryGetValue(opType, out os))
            {
                Console.Out.WriteLine("Cannot fetch opstats for " + opType);
                return 0;
            }
            else
            {
                return os.slowOps.Value;
            }
        }

        public void RecordSlowOp(String opType, long number) {
            POCopStats os;
            if (!opStats.TryGetValue(opType, out os))
            {
                Console.Out.WriteLine("Cannot fetch opstats for " + opType);
            }
            else
            {
                os.slowOps.Add(number);
            }
        }

        public void RecordOpsDone(String opType, long howmany) {
            POCopStats os;
            if (!opStats.TryGetValue(opType, out os)) {
                Console.Out.WriteLine("Cannot fetch opstats for " + opType);
            } else {
                os.totalOpsDone.Add(howmany);
            }
        }
    }
}