namespace FivetranClient.Infrastructure;

public class TtlDictionary<TKey, TValue> where TKey : notnull
{
    private readonly Dictionary<TKey, (TValue, DateTime)> _dictionary = new();

    public TValue GetOrAdd(TKey key, Func<TValue> valueFactory, TimeSpan ttl)
    {
        if (_dictionary.TryGetValue(key, out var entry))
        {
            if (DateTime.UtcNow < entry.Item2)
            {
                return entry.Item1;
            }

            _dictionary.Remove(key);
        }

        var value = valueFactory();
        _dictionary[key] = (value, DateTime.UtcNow.Add(ttl));
        return value;
    }
    public async Task<TValue> GetOrAddAsync(TKey key, Func<Task<TValue>> valueFactory, TimeSpan ttl)
    {
        if (_dictionary.TryGetValue(key, out var entry))
        {
            if (DateTime.UtcNow < entry.Item2)
                return entry.Item1;

            _dictionary.Remove(key);
        }

        var value = await valueFactory();
        _dictionary[key] = (value, DateTime.UtcNow.Add(ttl));
        return value;
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        if (_dictionary.TryGetValue(key, out var entry) && DateTime.UtcNow < entry.Item2)
        {
            value = entry.Item1;
            return true;
        }

        value = default!;
        return false;
    }
}