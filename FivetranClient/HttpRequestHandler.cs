using FivetranClient.Infrastructure;
using System.Net;

namespace FivetranClient;

public class HttpRequestHandler
{
    private readonly HttpClient _client;
    private readonly SemaphoreSlim? _semaphore;
    private readonly object _lock = new();
    private DateTime _retryAfterTime = DateTime.UtcNow;
    private static TtlDictionary<string, HttpResponseMessage> _responseCache = new();

    /// <summary>
    /// Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
    /// Also caches responses to avoid unnecessary network calls.
    /// </summary>
    /// <remarks>
    /// Set <paramref name="maxConcurrentRequests"/> to 0 to disable concurrency limit.
    /// </remarks>
    public HttpRequestHandler(HttpClient client, ushort maxConcurrentRequests = 0)
    {
        this._client = client;
        if (maxConcurrentRequests > 0)
        {
            this._semaphore = new SemaphoreSlim(maxConcurrentRequests, maxConcurrentRequests);
        }
    }

    public async Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
    {
        return _responseCache.GetOrAdd(
            url,
            () => this._GetAsync(url, cancellationToken).Result,
            TimeSpan.FromMinutes(60));
    }

    private async Task<HttpResponseMessage> _GetAsync(string url, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (_semaphore is not null)
            {
                await _semaphore.WaitAsync(cancellationToken);
            }

            try
            {
                TimeSpan wait;
                lock (_lock)
                {
                    wait = _retryAfterTime - DateTime.UtcNow;
                }

                if (wait > TimeSpan.Zero)
                {
                    await Task.Delay(wait, cancellationToken);
                }

                cancellationToken.ThrowIfCancellationRequested();

                var response = await _client.GetAsync(url, cancellationToken);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(60);
                    lock (_lock)
                    {
                        _retryAfterTime = DateTime.UtcNow.Add(retryAfter);
                    }

                    continue;
                }

                response.EnsureSuccessStatusCode();
                return response;
            }
            finally
            {
                _semaphore?.Release();
            }
        }
    }
}