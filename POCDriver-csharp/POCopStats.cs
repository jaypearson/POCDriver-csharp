//package com.johnlpage.pocdriver;


//import java.util.concurrent.atomic.AtomicInt64;
using Commons.Utils;

namespace POCDriver_csharp
{
    public class POCopStats {
        public AtomicInt64 intervalCount;
        public AtomicInt64 totalOpsDone;
        public AtomicInt64 slowOps;

        public POCopStats() {
            intervalCount = AtomicInt64.From(0);
            totalOpsDone = AtomicInt64.From(0);
            slowOps = AtomicInt64.From(0);
        }
    }
}
