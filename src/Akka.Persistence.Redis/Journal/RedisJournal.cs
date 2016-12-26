//-----------------------------------------------------------------------
// <copyright file="RedisJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;

using Akka.Actor;
using Akka.Util.Internal;

namespace Akka.Persistence.Redis.Journal
{
    using System.Reflection;

    using Akka.Configuration;
    using Akka.Routing;

    public class RedisJournal : ReceiveActor
    {
        private readonly RedisSettings _settings = RedisPersistence.Get(Context.System).JournalSettings;

        private IActorRef workers;

        public RedisJournal()
        {
            var extension = Persistence.Instance.Apply(Context.System);
            if (extension == null)
            {
                throw new ArgumentException(
                          "Couldn't initialize SyncWriteJournal instance, because associated Persistence extension has not been used in current actor system context.");
            }

            // dirty trick just for concept proof. Should be removed.
            // Config config = extension.ConfigFor(Self);
            Config config =
                (Config)typeof(PersistenceExtension).GetMethod("ConfigFor", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(extension, new []{ Self });

            this.workers =
                Context.ActorOf(
                    Props.Create(() => new RedisJournalWorker(config))
                        .WithRouter(new ConsistentHashingPool(_settings.MaxParallelism)),
                    "workers");

            this.Receive<WriteMessages>(
                m =>
                    {
                        var first = m.Messages.FirstOrDefault() as AtomicWrite;
                        if (first == null)
                        {
                            return;
                        }

                        this.workers.Forward(new ConsistentHashableEnvelope(m, first.PersistenceId));
                    });
            this.Receive<ReplayMessages>(m => this.workers.Forward(new ConsistentHashableEnvelope(m, m.PersistenceId)));
            this.Receive<ReadHighestSequenceNr>(
                m => this.workers.Forward(new ConsistentHashableEnvelope(m, m.PersistenceId)));
            this.Receive<DeleteMessagesTo>(
                m => this.workers.Forward(new ConsistentHashableEnvelope(m, m.PersistenceId)));
        }
    }
}