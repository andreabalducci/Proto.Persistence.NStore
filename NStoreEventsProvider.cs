using System;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace Proto.Persistence.NStore
{
    public class NStoreEventsProvider : IEventStore
    {
        private readonly IPersistence _events;
        
        public NStoreEventsProvider(IPersistence events)
        {
            _events = events;
        }
        
        public async Task<long> GetEventsAsync(string actorName, long indexStart, long indexEnd, Action<object> callback)
        {
            long lastIndex = -1;
            var adapter = new LambdaSubscription(chunk =>
            {
                lastIndex = chunk.Index;
                callback(chunk.Payload);
                return Task.FromResult(true);
            });

            await _events.ReadForwardAsync(actorName, indexStart, adapter, indexEnd).ConfigureAwait(false);
            return lastIndex; // TODO -> check sources, not really needed imho
        }

        public async Task<long> PersistEventAsync(string actorName, long index, object @event)
        {
            await _events.AppendAsync(actorName, index, @event).ConfigureAwait(false);
            return index++; // TODO -> check sources, not really needed imho
        }

        public async Task DeleteEventsAsync(string actorName, long inclusiveToIndex)
        {
            await _events.DeleteAsync(actorName, 0, inclusiveToIndex).ConfigureAwait(false);
        }
    }
}
