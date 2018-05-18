using System;
using System.Collections.Generic;
using CommandLine;
//Yes - lots of public values, getters are OTT here.

namespace POCDriver_csharp
{
    public class POCTestOptions
    {
        public const long NUMBER_SIZE = 1000000;
        public int rangequeries = 0;
        public int numShards = 1;
        public Boolean sharded = false;
        public int secondaryidx = 0;

        [Option('a', HelpText= "Shape of any arrays in new sample records x:y so -a 12:60 adds an array of 12 length 60 arrays of Int32s", Separator = ':', Default = new int[] { 2, 2 } )]
        public IList<int> arrays { get; set; }

        [Option('b', "batchsize", HelpText = "Bulk op size", Default = 512)]
        public int batchSize { get; set; }

        [Option('c', HelpText = "Mongodb connection details", Default = "mongodb://localhost:27017")]
        public string host { get; set; }

        [Option('d', HelpText = "Test duration in seconds", Default = 18000)]
        public int duration { get; set; }

        [Option('e', "empty", HelpText = "Remove data from collection on startup", Default = false)]
        public bool emptyFirst { get; set; }

        [Option('f', HelpText = "Number of top level fields in test records", Default = 10)]
        public int numFields { get; set; }

        [Option(HelpText = "The depth of the document created", Default = 0)]
        public int depth { get; set; }

        [Option('g', HelpText = "Ratio of array increment ops requires option 'a'", Default = 0)]
        public int arrayupdates { get; set; }

        [Option('i', "inserts", HelpText = "Ratio of insert operations", Default = 100)]
        public int insertops { get; set; }

        [Option('j', HelpText = "Percentage of database to be the working set", Default = 100)]
        public int workingset { get; set; }

        [Option('k', HelpText = "Ratio of key query operations", Default = 0)]
        public int keyqueries { get; set; }

        [Option('l', HelpText = "Length of text fields in bytes", Default = 30)]
        public int textFieldLen { get; set; }

        [Option('m', "findandmodify", HelpText = "Use findAndModify instead of update and retrieve record (with -u or -v only)", Default = false)]
        public bool findandmodify { get; set; }

        [Option('n', "namespace", HelpText = "Namespace to use, for example myDatabase.myCollection", Default = new string[] { "POCDB", "POCCOLL" }, Separator = '.')]
        public IList<string> namespaces { get; set;}

        [Option('o', HelpText = "Log output to <filename>")]
        public string logfile { get; set; }

        [Option('p', "print", HelpText = "Print out a sample record according to the other parameters then quit", Default = false)]
        public bool printOnly { get; set; }

        [Option('q', HelpText = "Try to rate limit the total ops/s to the specified amount", Default = 0)]
        public int opsPerSecond { get; set; }

        [Option('r', "reporttime", HelpText = "Delay in seconds between status reports", Default = 10)]
        public int reportTime { get; set; }
        
        [Option('s', "slowthreshold", HelpText = "Slow operation threshold in ms", Default = 50)]
        public int slowThreshold { get; set; }

        [Option('t', "threads", HelpText = "Number of threads", Default = 4)]
        public int numThreads { get; set; }

        [Option('u', HelpText = "Ratio of update operations", Default = 100)]
        public int updates { get; set; }

        [Option('v', HelpText = "Specify a set of ordered operations per thread from [iukp]", Default = "i")]
        public string workflow { get; set; }

        [Option("nosharding", HelpText = "Do not shard the collection", Default = false)]
        public bool singleserver { get; set; }

        [Option('y', "collections", HelpText = "Number of collections to span the workload over, implies w", Default = 1)]
        public int numcollections { get; set; }

        [Option('z', "zipfian", HelpText = "Enable zipfian distribution over X number of documents", Default = 0)]
        public int zipfsize { get; set; }

        [Option(HelpText = "Start 'workerId' for each thread. 'w' value in _id", Default = 0)]
        public int threadIdStart { get; set; }

        [Option("fulltext", HelpText = "Create fulltext index", Default = false)]
        public bool fulltext { get; set; }

        [Option("binary", HelpText = "add a binary blob of size KB", Default = 0)]
        public int blobSize { get; set; }

        [Option("rangedocs", HelpText = "Number of documents to fetch for range queries ", Default = 10)]
        public int rangeDocs { get; set; }

        [Option("updatefields", HelpText = "Number of fields to update", Default = 1)]
        public int updateFields { get; set; }

        [Option("projectfields", HelpText = "Number of fields to project in finds", Default = 0)]
        public int projectFields { get; set; }

        [Option("debug", HelpText = "Show more detail if exceptions occur during inserts/queries", Default = false)]
        public bool debug { get; set; }

        //public POCTestOptions(String[] args)
        //{
        //    var cmd = Parser.Default.ParseArguments(args);

            //if (cmd.hasOption("binary"))
            //{
            //    blobSize = Int32.Parse(cmd.getOptionValue("binary"));
            //}

            //if (cmd.hasOption("q"))
            //{
            //    opsPerSecond = Int32.Parse(cmd.getOptionValue("q"));
            //}

            //if (cmd.hasOption("j"))
            //{
            //    workingset = Int32.Parse(cmd.getOptionValue("j"));
            //}

            //if (cmd.hasOption("v"))
            //{
            //    workflow = cmd.getOptionValue("v");
            //}

            //if (cmd.hasOption("n"))
            //{
            //    String ns = cmd.getOptionValue("n");
            //    String[] parts = ns.Split("\\.");
            //    if (parts.Length != 2)
            //    {
            //        Console.Error.WriteLine("namespace format is 'DATABASE.COLLECTION' ");
            //        Environment.Exit(1);
            //    }
            //    namespaces[0] = parts[0];
            //    namespaces[1] = parts[1];
            //}

            //if (cmd.hasOption("a"))
            //{
            //    String ao = cmd.getOptionValue("a");
            //    String[] parts = ao.Split(':');
            //    if (parts.Length != 2)
            //    {
            //        Console.Error.WriteLine("array format is 'top:second'");
            //        Environment.Exit(1);
            //    }
            //    arrays[0] = Int32.Parse(parts[0]);
            //    arrays[1] = Int32.Parse(parts[1]);
            //}

            //if (cmd.hasOption("e"))
            //{
            //    emptyFirst = true;
            //}


            //if (cmd.hasOption("p"))
            //{
            //    printOnly = true;
            //}

            //if (cmd.hasOption("w"))
            //{
            //    singleserver = true;
            //}
            //if (cmd.hasOption("r"))
            //{
            //    rangequeries = Int32.Parse(cmd.getOptionValue("r"));
            //}

            //if (cmd.hasOption("d"))
            //{
            //    duration = Int32.Parse(cmd.getOptionValue("d"));
            //}

            //if (cmd.hasOption("g"))
            //{
            //    arrayupdates = Int32.Parse(cmd.getOptionValue("g"));
            //}

            //if (cmd.hasOption("u"))
            //{
            //    updates = Int32.Parse(cmd.getOptionValue("u"));
            //}

            //if (cmd.hasOption("i"))
            //{
            //    insertops = Int32.Parse(cmd.getOptionValue("i"));
            //}

            //if (cmd.hasOption("x"))
            //{
            //    secondaryidx = Int32.Parse(cmd.getOptionValue("x"));
            //}
            //if (cmd.hasOption("y"))
            //{
            //    numcollections = Int32.Parse(cmd.getOptionValue("y"));
            //    singleserver = true;
            //}
            //if (cmd.hasOption("z"))
            //{
            //    zipfian = true;
            //    zipfsize = Int32.Parse(cmd.getOptionValue("z"));
            //}

            //if (cmd.hasOption("o"))
            //{
            //    logfile = cmd.getOptionValue("o");
            //}

            //if (cmd.hasOption("k"))
            //{
            //    keyqueries = Int32.Parse(cmd.getOptionValue("k"));
            //}

            //if (cmd.hasOption("b"))
            //{
            //    batchSize = Int32.Parse(cmd.getOptionValue("b"));
            //}

            //if (cmd.hasOption("s"))
            //{
            //    slowThreshold = Int32.Parse(cmd.getOptionValue("s"));
            //}

            //// automatically generate the help statement
            //if (cmd.hasOption("h"))
            //{
            //    HelpFormatter formatter = new HelpFormatter();
            //    formatter.printHelp("POCDriver", cliopt);
            //    helpOnly = true;
            //}

            //if (cmd.hasOption("c"))
            //{
            //    connectionDetails = cmd.getOptionValue("c");
            //}

            //if (cmd.hasOption("l"))
            //{
            //    textFieldLen = Int32.Parse(cmd.getOptionValue("l"));
            //}

            //if (cmd.hasOption("m"))
            //{
            //    findandmodify = true;
            //}

            //if (cmd.hasOption("f"))
            //{
            //    numFields = Int32.Parse(cmd.getOptionValue("f"));
            //}

            //if (cmd.hasOption("depth"))
            //{
            //    depth = Int32.Parse(cmd.getOptionValue("depth"));
            //}

            //if (cmd.hasOption("o"))
            //{
            //    statsfile = cmd.getOptionValue("o");
            //}

            //if (cmd.hasOption("t"))
            //{
            //    numThreads = Int32.Parse(cmd.getOptionValue("t"));
            //}
            //if (cmd.hasOption("fulltext"))
            //{
            //    fulltext = true;
            //}

            //if (cmd.hasOption("threadIdStart"))
            //{
            //    threadIdStart = Int32.Parse(cmd.getOptionValue("threadIdStart"));
            //}

            //if (cmd.hasOption("rangedocs"))
            //{
            //    rangeDocs = Int32.Parse(cmd.getOptionValue("rangedocs"));
            //}

            //if (cmd.hasOption("updatefields"))
            //{
            //    updateFields = Int32.Parse(cmd.getOptionValue("updatefields"));
            //}

            //if (cmd.hasOption("projectfields"))
            //{
            //    projectFields = Int32.Parse(cmd.getOptionValue("projectfields"));
            //}

            //if (cmd.hasOption("debug"))
            //{
            //    debug = true;
            //}
        // }
    }
}