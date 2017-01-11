//-----------------------------------------------------------------------
// <copyright file="RedisJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Configuration;
using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Redis.Tests
{
    [Collection("RedisSpec")]
    public class RedisJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig;
        private static readonly string KeyPrefix;

        static RedisJournalSpec()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["redis"].ConnectionString;
            var database = ConfigurationManager.AppSettings["redisDatabase"];

            SpecConfig = ConfigurationFactory.ParseString(@"
                akka.test.single-expect-default = 3s
                akka.persistence {
                    publish-plugin-commands = on
                    journal {
                        plugin = ""akka.persistence.journal.redis""
                        redis {
                            class = ""Akka.Persistence.Redis.Journal.RedisJournal, Akka.Persistence.Redis""
                            configuration-string = """ + connectionString + @"""
                            plugin-dispatcher = ""akka.persistence.journal.redis.dispatcher""
                            dispatcher {
                                type = PinnedDispatcher
                            }
                            database = """ + database + @"""
                            key-prefix = ""akka:persistence:journal""
                        }
                    }
                }");

            KeyPrefix = SpecConfig.GetString("akka.persistence.journal.redis.key-prefix");
        }

        public RedisJournalSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(RedisJournalSpec).Name, output)
        {
            RedisPersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.Clean(KeyPrefix);
        }
    }
}