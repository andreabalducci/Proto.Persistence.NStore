using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace Proto.Persistence.NStore
{
    public class NStoreSnapshotStore : ISnapshotStore
    {
        private readonly IPersistence _snapshots;

        public NStoreSnapshotStore(IPersistence snapshots)
        {
            this._snapshots = snapshots;
        }

        public async Task<(object Snapshot, long Index)> GetSnapshotAsync(string actorName)
        {
            var last = await _snapshots.ReadSingleBackwardAsync(actorName).ConfigureAwait(false);

            return last != null ? (last.Payload, last.Index) : (null, 0);
        }

        public async Task PersistSnapshotAsync(string actorName, long index, object snapshot)
        {
            await _snapshots.AppendAsync(actorName, index, snapshot).ConfigureAwait(false);
        }

        public async Task DeleteSnapshotsAsync(string actorName, long inclusiveToIndex)
        {
            await _snapshots.DeleteAsync(actorName, 0, inclusiveToIndex).ConfigureAwait(false);
        }
    }
}