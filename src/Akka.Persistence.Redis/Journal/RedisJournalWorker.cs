//-----------------------------------------------------------------------
// <copyright file="RedisJournalWorker.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

namespace Akka.Persistence.Redis.Journal
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Threading.Tasks;

    using Akka.Actor;
    using Akka.Configuration;
    using Akka.Persistence.Journal;
    using Akka.Serialization;

    using StackExchange.Redis;

    public class RedisJournalWorker : SyncWriteJournal
    {
        private readonly RedisSettings _settings;
        private Lazy<Serializer> _serializer;
        private Lazy<IDatabase> _database;
        private ActorSystem _system;

        public RedisJournalWorker(Config config) : base(config)
        {
            this._settings = RedisPersistence.Get(Context.System).JournalSettings;
        }

        protected override void PreStart()
        {
            base.PreStart();
            this._system = Context.System;
            this._database = new Lazy<IDatabase>(() =>
                            {
                                var redisConnection = ConnectionMultiplexer.Connect(this._settings.ConfigurationString);
                                return redisConnection.GetDatabase(this._settings.Database);
                            });
            this._serializer = new Lazy<Serializer>(() => this._system.Serialization.FindSerializerForType(typeof(JournalEntry)));
        }

        public override void ReplayMessages(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            RedisValue[] journals = this._database.Value.SortedSetRangeByScore(this.GetJournalKey(persistenceId), fromSequenceNr, toSequenceNr, skip: 0L, take: max);

            foreach (var journal in journals)
            {
                recoveryCallback(this.ToPersistenceRepresentation(this._serializer.Value.FromBinary<JournalEntry>(journal), context.Sender));
            }
        }

        public override long ReadHighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            var highestSequenceNr = this._database.Value.StringGet(this.GetHighestSequenceNrKey(persistenceId));
            return highestSequenceNr.IsNull ? 0L : (long)highestSequenceNr;
        }

        protected override void DeleteMessagesTo(string persistenceId, long toSequenceNr)
        {
            this._database.Value.SortedSetRemoveRangeByScore(this.GetJournalKey(persistenceId), -1, toSequenceNr);
        }

        protected override IImmutableList<Exception> WriteMessages(IEnumerable<AtomicWrite> messages)
        {
            var messagesList = messages.ToList();

            var result = messagesList.GroupBy(x => x.PersistenceId).ToDictionary(m => m.Key,
                g =>
                    {
                        try
                        {
                            var persistentMessages = g.SelectMany(aw => (IImmutableList<IPersistentRepresentation>)aw.Payload).ToList();

                            var persistenceId = g.Key;
                            var highSequenceId = persistentMessages.Max(c => c.SequenceNr);

                            var transaction = this._database.Value.CreateTransaction();

                            foreach (var write in persistentMessages)
                            {
                                transaction.SortedSetAddAsync(this.GetJournalKey(write.PersistenceId), this._serializer.Value.ToBinary(this.ToJournalEntry(write)), write.SequenceNr);
                            }

                            transaction.StringSetAsync(this.GetHighestSequenceNrKey(persistenceId), highSequenceId);

                            if (!transaction.Execute())
                            {
                                throw new Exception($"{nameof(this.WriteMessages)}: failed to write {typeof(JournalEntry).Name} to redis");
                            }

                            return null;
                        }
                        catch (Exception e)
                        {
                            return this.TryUnwrapException(e);
                        }
                    });

            return messages.Select(m => result[m.PersistenceId]).ToImmutableList();
        }

        private RedisKey GetJournalKey(string persistenceId) => $"{this._settings.KeyPrefix}:{persistenceId}";

        private RedisKey GetHighestSequenceNrKey(string persistenceId)
        {
            return $"{this.GetJournalKey(persistenceId)}.highestSequenceNr";
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            return new JournalEntry
                       {
                           PersistenceId = message.PersistenceId,
                           SequenceNr = message.SequenceNr,
                           IsDeleted = message.IsDeleted,
                           Payload = message.Payload,
                           Manifest = message.Manifest
                       };
        }

        private Persistent ToPersistenceRepresentation(JournalEntry entry, IActorRef sender)
        {
            return new Persistent(entry.Payload, entry.SequenceNr, entry.PersistenceId, entry.Manifest, entry.IsDeleted, sender);
        }
    }
}