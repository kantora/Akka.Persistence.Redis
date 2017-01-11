using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.Redis.Journal
{
    using System.Collections.Immutable;

    using Akka.Actor;
    using Akka.Configuration;
    using Akka.Pattern;
    using Akka.Persistence.Journal;

    public abstract class SyncWriteJournal : WriteJournalBase
    {
        private static readonly TaskContinuationOptions _continuationOptions =
            TaskContinuationOptions.ExecuteSynchronously;

        protected readonly bool CanPublish;

        private readonly CircuitBreaker _breaker;

        private readonly ReplayFilterMode _replayFilterMode;

        private readonly bool _isReplayFilterEnabled;

        private readonly int _replayFilterWindowSize;

        private readonly int _replayFilterMaxOldWriters;

        private readonly bool _replayDebugEnabled;

        private readonly IActorRef _resequencer;

        private long _resequencerCounter = 1L;

        protected SyncWriteJournal(Config config)
        {
            var extension = Persistence.Instance.Apply(Context.System);
            if (extension == null)
            {
                throw new ArgumentException(
                          "Couldn't initialize SyncWriteJournal instance, because associated Persistence extension has not been used in current actor system context.");
            }

            CanPublish = extension.Settings.Internal.PublishPluginCommands;
            
            //Config config = extension.ConfigFor(Self);
            _breaker = new CircuitBreaker(
                           config.GetInt("circuit-breaker.max-failures"),
                           config.GetTimeSpan("circuit-breaker.call-timeout"),
                           config.GetTimeSpan("circuit-breaker.reset-timeout"));

            var replayFilterMode = config.GetString("replay-filter.mode").ToLower();
            switch (replayFilterMode)
            {
                case "off":
                    _replayFilterMode = ReplayFilterMode.Disabled;
                    break;
                case "repair-by-discard-old":
                    _replayFilterMode = ReplayFilterMode.RepairByDiscardOld;
                    break;
                case "fail":
                    _replayFilterMode = ReplayFilterMode.Fail;
                    break;
                case "warn":
                    _replayFilterMode = ReplayFilterMode.Warn;
                    break;
                default:
                    throw new ArgumentException(
                              string.Format(
                                  "Invalid replay-filter.mode [{0}], supported values [off, repair-by-discard-old, fail, warn]",
                                  replayFilterMode));
            }
            _isReplayFilterEnabled = _replayFilterMode != ReplayFilterMode.Disabled;
            _replayFilterWindowSize = config.GetInt("replay-filter.window-size");
            _replayFilterMaxOldWriters = config.GetInt("replay-filter.max-old-writers");
            _replayDebugEnabled = config.GetBoolean("replay-filter.debug");

            _resequencer = Context.ActorOf(Props.Create(() => new Resequencer()));
        }

        public abstract void ReplayMessages(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback);

        public abstract long ReadHighestSequenceNr(string persistenceId, long fromSequenceNr);

        /// <summary>
        /// Plugin API: asynchronously writes a batch of persistent messages to the
        /// journal.
        /// 
        /// The batch is only for performance reasons, i.e. all messages don't have to be written
        /// atomically. Higher throughput can typically be achieved by using batch inserts of many
        /// records compared to inserting records one-by-one, but this aspect depends on the
        /// underlying data store and a journal implementation can implement it as efficient as
        /// possible. Journals should aim to persist events in-order for a given `persistenceId`
        /// as otherwise in case of a failure, the persistent state may be end up being inconsistent.
        /// 
        /// Each <see cref="AtomicWrite"/> message contains the single <see cref="Persistent"/>
        /// that corresponds to the event that was passed to the 
        /// <see cref="Eventsourced.Persist{TEvent}(TEvent,Action{TEvent})"/> method of the
        /// <see cref="PersistentActor" />, or it contains several <see cref="Persistent"/>
        /// that correspond to the events that were passed to the
        /// <see cref="Eventsourced.PersistAll{TEvent}(IEnumerable{TEvent},Action{TEvent})"/>
        /// method of the <see cref="PersistentActor"/>. All <see cref="Persistent"/> of the
        /// <see cref="AtomicWrite"/> must be written to the data store atomically, i.e. all or none must
        /// be stored. If the journal (data store) cannot support atomic writes of multiple
        /// events it should reject such writes with a <see cref="NotSupportedException"/>
        /// describing the issue. This limitation should also be documented by the journal plugin.
        /// 
        /// If there are failures when storing any of the messages in the batch the returned
        /// <see cref="Task"/> must be completed with failure. The <see cref="Task"/> must only be completed with
        /// success when all messages in the batch have been confirmed to be stored successfully,
        /// i.e. they will be readable, and visible, in a subsequent replay. If there is
        /// uncertainty about if the messages were stored or not the <see cref="Task"/> must be completed
        /// with failure.
        /// 
        /// Data store connection problems must be signaled by completing the <see cref="Task"/> with
        /// failure.
        /// 
        /// The journal can also signal that it rejects individual messages (<see cref="AtomicWrite"/>) by
        /// the returned <see cref="Task"/>. It is possible but not mandatory to reduce
        /// number of allocations by returning null for the happy path,
        /// i.e. when no messages are rejected. Otherwise the returned list must have as many elements
        /// as the input <paramref name="messages"/>. Each result element signals if the corresponding
        /// <see cref="AtomicWrite"/> is rejected or not, with an exception describing the problem. Rejecting
        /// a message means it was not stored, i.e. it must not be included in a later replay.
        /// Rejecting a message is typically done before attempting to store it, e.g. because of
        /// serialization error.
        /// 
        /// Data store connection problems must not be signaled as rejections.
        /// 
        /// It is possible but not mandatory to reduce number of allocations by returning
        /// null for the happy path, i.e. when no messages are rejected.
        /// 
        /// Calls to this method are serialized by the enclosing journal actor. If you spawn
        /// work in asyncronous tasks it is alright that they complete the futures in any order,
        /// but the actual writes for a specific persistenceId should be serialized to avoid
        /// issues such as events of a later write are visible to consumers (query side, or replay)
        /// before the events of an earlier write are visible.
        /// A <see cref="PersistentActor"/> will not send a new <see cref="WriteMessages"/> request before
        /// the previous one has been completed.
        /// 
        /// Please not that the <see cref="IPersistentRepresentation.Sender"/> of the contained
        /// <see cref="Persistent"/> objects has been nulled out (i.e. set to <see cref="ActorRefs.NoSender"/>
        /// in order to not use space in the journal for a sender reference that will likely be obsolete
        /// during replay.
        /// 
        /// Please also note that requests for the highest sequence number may be made concurrently
        /// to this call executing for the same `persistenceId`, in particular it is possible that
        /// a restarting actor tries to recover before its outstanding writes have completed.
        /// In the latter case it is highly desirable to defer reading the highest sequence number
        /// until all outstanding writes have completed, otherwise the <see cref="PersistentActor"/>
        /// may reuse sequence numbers.
        /// 
        /// This call is protected with a circuit-breaker.
        /// </summary>
        protected abstract IImmutableList<Exception> WriteMessages(IEnumerable<AtomicWrite> messages);

        /// <summary>
        /// Asynchronously deletes all persistent messages up to inclusive <paramref name="toSequenceNr"/>
        /// bound.
        /// </summary>
        protected abstract void DeleteMessagesTo(string persistenceId, long toSequenceNr);

        /// <summary>
        /// Allows plugin implementers to use <see cref="PipeToSupport.PipeTo{T}"/> <see cref="ActorBase.Self"/>
        /// and handle additional messages for implementing advanced features
        /// </summary>
        protected virtual bool ReceivePluginInternal(object message)
        {
            return false;
        }

        protected sealed override bool Receive(object message)
        {
            return ReceiveWriteJournal(message) || ReceivePluginInternal(message);
        }

        protected bool ReceiveWriteJournal(object message)
        {
            if (message is WriteMessages) HandleWriteMessages((WriteMessages)message);
            else if (message is ReplayMessages) HandleReplayMessages((ReplayMessages)message);
            else if (message is ReadHighestSequenceNr) HandleReadHighestSequenceNr((ReadHighestSequenceNr)message);
            else if (message is DeleteMessagesTo) HandleDeleteMessagesTo((DeleteMessagesTo)message);
            else return false;
            return true;
        }

        private void HandleDeleteMessagesTo(DeleteMessagesTo message)
        {
            var eventStream = Context.System.EventStream;
            object result;
            try
            {
                DeleteMessagesTo(message.PersistenceId, message.ToSequenceNr);
                result = new DeleteMessagesSuccess(message.ToSequenceNr);
                if (this.CanPublish)
                {
                    eventStream.Publish(message);
                }
            }
            catch (Exception e)
            {
                result = new DeleteMessagesFailure(TryUnwrapException(e), message.ToSequenceNr);
            }

            message.PersistentActor.Tell(result);
        }

        private void HandleReadHighestSequenceNr(ReadHighestSequenceNr message)
        {
            object result;
            try
            {
                // Send read highest sequence number to persistentActor directly. No need
                // to resequence the result relative to written and looped messages.
                result =
                    new ReadHighestSequenceNrSuccess(
                        ReadHighestSequenceNr(message.PersistenceId, message.FromSequenceNr));
            }
            catch (Exception exception)
            {
                result = new ReadHighestSequenceNrFailure(TryUnwrapException(exception));
            }

            message.PersistentActor.Tell(result);
        }

        private void HandleReplayMessages(ReplayMessages message)
        {
            var replyTo = _isReplayFilterEnabled
                              ? Context.ActorOf(
                                  ReplayFilter.Props(
                                      message.PersistentActor,
                                      _replayFilterMode,
                                      _replayFilterWindowSize,
                                      _replayFilterMaxOldWriters,
                                      _replayDebugEnabled))
                              : message.PersistentActor;
            var context = Context;
            var readHighestSequenceNrFrom = Math.Max(0, message.FromSequenceNr - 1);
            long resultHighSequenceNr;
            object result;
            try
            {
                var highSequenceNr = this._breaker.WithSyncCircuitBreaker(
                    () => ReadHighestSequenceNr(message.PersistenceId, readHighestSequenceNrFrom));
                var toSequenceNr = Math.Min(message.ToSequenceNr, highSequenceNr);
                if (highSequenceNr == 0 || message.FromSequenceNr > toSequenceNr)
                {
                    resultHighSequenceNr = 0L;
                }
                else
                {
                    ReplayMessages(
                        context,
                        message.PersistenceId,
                        message.FromSequenceNr,
                        toSequenceNr,
                        message.Max,
                        p =>
                            {
                                if (!p.IsDeleted) // old records from pre 1.5 may still have the IsDeleted flag
                                {
                                    foreach (var adaptedRepresentation in AdaptFromJournal(p))
                                    {
                                        replyTo.Tell(new ReplayedMessage(adaptedRepresentation), ActorRefs.NoSender);
                                    }
                                }
                            });
                    resultHighSequenceNr = highSequenceNr;
                }

                result = new RecoverySuccess(resultHighSequenceNr);
                if (this.CanPublish)
                {
                    context.System.EventStream.Publish(message);
                }
            }
            catch (Exception exception)
            {
                result = new ReplayMessagesFailure(TryUnwrapException(exception));
            }

            replyTo.Tell(result);
        }

        protected Exception TryUnwrapException(Exception e)
        {
            var aggregateException = e as AggregateException;
            if (aggregateException != null)
            {
                aggregateException = aggregateException.Flatten();
                if (aggregateException.InnerExceptions.Count == 1)
                    return aggregateException.InnerExceptions[0];
            }
            return e;
        }

        private void HandleWriteMessages(WriteMessages message)
        {
            var counter = _resequencerCounter;

            /*
             * Self MUST BE CLOSED OVER here, or the code below will be subject to race conditions which may result
             * in failure, as the `IActorContext` needed for resolving Context.Self will be done outside the current
             * execution context.
             */
            var self = Self;
            _resequencerCounter += message.Messages.Aggregate(1, (acc, m) => acc + m.Size);
            var atomicWriteCount = message.Messages.OfType<AtomicWrite>().Count();
            AtomicWrite[] prepared;
            IImmutableList<Exception> writeResult;
            Exception writeMessagesException = null;
            try
            {
                prepared = PreparePersistentBatch(message.Messages).ToArray();
                // try in case AsyncWriteMessages throws
                try
                {
                    writeResult = _breaker.WithSyncCircuitBreaker(() => WriteMessages(prepared));
                }
                catch (Exception e)
                {
                    writeResult = null;
                    writeMessagesException = e;
                }
            }
            catch (Exception e)
            {
                // exception from PreparePersistentBatch => rejected
                writeResult = Enumerable.Repeat(e, atomicWriteCount).ToImmutableList();
            }

            Action<Func<IPersistentRepresentation, Exception, object>, IImmutableList<Exception>> resequence = (mapper, results) =>
            {
                var i = 0;
                var enumerator = results != null ? results.GetEnumerator() : null;
                foreach (var resequencable in message.Messages)
                {
                    if (resequencable is AtomicWrite)
                    {
                        var aw = resequencable as AtomicWrite;
                        Exception exception = null;
                        if (enumerator != null)
                        {
                            enumerator.MoveNext();
                            exception = enumerator.Current;
                        }
                        foreach (var p in (IEnumerable<IPersistentRepresentation>)aw.Payload)
                        {
                            _resequencer.Tell(new Desequenced(mapper(p, exception), counter + i + 1, message.PersistentActor, p.Sender));
                            i++;
                        }
                    }
                    else
                    {
                        var loopMsg = new LoopMessageSuccess(resequencable.Payload, message.ActorInstanceId);
                        _resequencer.Tell(new Desequenced(loopMsg, counter + i + 1, message.PersistentActor,
                            resequencable.Sender));
                        i++;
                    }
                }
            };

            if (writeResult != null && writeResult.Count != atomicWriteCount)
            {
                throw new IllegalStateException(string.Format("WriteMessages return invalid number or results. " +
                                                "Expected [{0}], but got [{1}].", atomicWriteCount, writeResult.Count));
            }

            if (writeMessagesException == null)
            {
                _resequencer.Tell(
                    new Desequenced(WriteMessagesSuccessful.Instance, counter, message.PersistentActor, self));
                resequence(
                    (x, exception) =>
                        exception == null
                            ? (object)new WriteMessageSuccess(x, message.ActorInstanceId)
                            : new WriteMessageRejected(x, exception, message.ActorInstanceId),
                    writeResult);
            }
            else
            {
                _resequencer.Tell(new Desequenced(new WriteMessagesFailed(writeMessagesException), counter, message.PersistentActor, self));
                resequence((x, _) => new WriteMessageFailure(x, writeMessagesException, message.ActorInstanceId), null);
            }
        }

        internal sealed class Desequenced
        {
            public Desequenced(object message, long sequenceNr, IActorRef target, IActorRef sender)
            {
                Message = message;
                SequenceNr = sequenceNr;
                Target = target;
                Sender = sender;
            }

            public readonly object Message;
            public readonly long SequenceNr;
            public readonly IActorRef Target;
            public readonly IActorRef Sender;
        }

        internal class Resequencer : ActorBase
        {
            private readonly IDictionary<long, Desequenced> _delayed = new Dictionary<long, Desequenced>();
            private long _delivered = 0L;

            protected override bool Receive(object message)
            {
                Desequenced d;
                if ((d = message as Desequenced) != null)
                {
                    do
                    {
                        d = Resequence(d);
                    } while (d != null);
                    return true;
                }
                return false;
            }

            private Desequenced Resequence(Desequenced desequenced)
            {
                if (desequenced.SequenceNr == _delivered + 1)
                {
                    _delivered = desequenced.SequenceNr;
                    desequenced.Target.Tell(desequenced.Message, desequenced.Sender);
                }
                else
                {
                    _delayed.Add(desequenced.SequenceNr, desequenced);
                }

                Desequenced d;
                var delivered = _delivered + 1;
                if (_delayed.TryGetValue(delivered, out d))
                {
                    _delayed.Remove(delivered);
                    return d;
                }

                return null;
            }
        }
    }
}
