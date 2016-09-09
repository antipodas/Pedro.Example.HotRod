using Google.Protobuf;
using Infinispan.HotRod;
using Infinispan.HotRod.Config;
using Org.Infinispan.Protostream;
using Org.Infinispan.Query.Remote.Client;
using SampleBankAccount;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pedro.Test.Hotrod
{
    class Program
    {
        static void Main(string[] args)
        {
            // Cache manager setup
            RemoteCacheManager remoteManager;
            const string ERRORS_KEY_SUFFIX = ".errors";
            const string PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";
            ConfigurationBuilder conf = new ConfigurationBuilder();
            conf.AddServer().Host("127.0.0.1").Port(11222).ConnectionTimeout(90000).SocketTimeout(6000);
            conf.Marshaller(new BasicTypesProtoStreamMarshaller());
            remoteManager = new RemoteCacheManager(conf.Build(), true);
            IRemoteCache<String, String> metadataCache = remoteManager.GetCache<String, String>(PROTOBUF_METADATA_CACHE_NAME);
            IRemoteCache<int, User> testCache = remoteManager.GetCache<int, User>("namedCache");

            // This example continues the previous codeblock
            // Installing the entities model into the Infinispan __protobuf_metadata cache    
            metadataCache.Put("sample_bank_account/bank.proto", File.ReadAllText("../../resources/proto2/bank.proto"));

            if (metadataCache.ContainsKey(ERRORS_KEY_SUFFIX))
            {
                Console.WriteLine("fail: error in registering .proto model");
                Environment.Exit(-1);
            }

            // This example continues the previous codeblock
            // The application cache must contain entities only 
            testCache.Clear();
            // Fill the application cache
            User user1 = new User();
            user1.Id = 4;
            user1.Name = "Jerry";
            user1.Surname = "Mouse";
            User ret = testCache.Put(4, user1);

            User user2 = new User();
            user2.Id = 5;
            user2.Name = "Micky";
            user2.Surname = "Mouse";
            User ret2 = testCache.Put(5, user2);

            // This example continues the previous codeblock
            // Run a query
            QueryRequest qr = new QueryRequest();
            qr.JpqlString = "from sample_bank_account.User where surname like '%ous%'";
            QueryResponse result = testCache.Query(qr);
            List<User> listOfUsers = new List<User>();
            unwrapResults(result, listOfUsers);
            Console.WriteLine("There are " + listOfUsers.Count + " Users:");
            foreach (User user in listOfUsers)
            {
                Console.WriteLine("####################");
                Console.WriteLine("User ID: " + user.Id);
                Console.WriteLine("User Name: " + user.Name);
                Console.WriteLine("User Surname: " + user.Surname);
                Console.WriteLine("####################");
                System.Threading.Thread.Sleep(1000);
            }
            System.Threading.Thread.Sleep(5000);

        }

        // Convert Protobuf matter into C# objects
        private static bool unwrapResults<T>(QueryResponse resp, List<T> res) where T : IMessage<T>
        {
            if (resp.ProjectionSize > 0)
            {  // Query has select
                return false;
            }
            for (int i = 0; i < resp.NumResults; i++)
            {
                WrappedMessage wm = resp.Results.ElementAt(i);

                if (wm.WrappedBytes != null)
                {
                    WrappedMessage wmr = WrappedMessage.Parser.ParseFrom(wm.WrappedBytes);
                    if (wmr.WrappedMessageBytes != null)
                    {

                        System.Reflection.PropertyInfo pi = typeof(T).GetProperty("Parser");

                        MessageParser<T> p = (MessageParser<T>)pi.GetValue(null);
                        T u = p.ParseFrom(wmr.WrappedMessageBytes);
                        res.Add(u);
                    }
                }
            }
            return true;
        }
    }
}
