using System.Net.Http.Headers;
using System.Text;

namespace FivetranClient.Infrastructure;

public class FivetranHttpClient : HttpClient
{
    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret, TimeSpan timeout)
    {
        if (timeout.Ticks <= 0)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be a positive value");

        this.DefaultRequestHeaders.Clear();
        this.BaseAddress = baseAddress;
        this.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic", CalculateToken(apiKey, apiSecret));
        this.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        // we need to set Agent Header because otherwise sometimes it may be blocked by the server
        // see: https://repost.aws/knowledge-center/waf-block-http-requests-no-user-agent
        this.DefaultRequestHeaders.UserAgent.ParseAdd("aseduigbn");
        this.Timeout = timeout;
    }

    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret)
        : this(baseAddress, apiKey, apiSecret, TimeSpan.FromSeconds(40))
    {
    }

    public static string CalculateToken(string apiKey, string apiSecret)
    {
        return Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:{apiSecret}"));
    }
}