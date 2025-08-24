using FivetranClient;
using Import.Helpers.Fivetran;
using System.Text;

namespace Import.ConnectionSupport;

// equivalent of database is group in Fivetran terminology
public class FivetranConnectionSupport : IConnectionSupport
{
    public const string ConnectorTypeCode = "FIVETRAN";
    private record FivetranConnectionDetailsForSelection(string ApiKey, string ApiSecret);

    public object? GetConnectionDetailsForSelection()
    {
        Console.Write("Provide your Fivetran API Key: ");
        var apiKey = Console.ReadLine() ?? throw new ArgumentNullException();
        Console.Write("Provide your Fivetran API Secret: ");
        var apiSecret = Console.ReadLine() ?? throw new ArgumentNullException();

        return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
    }

    public object GetConnection(object? connectionDetails, string? selectedToImport)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }

        return new RestApiManagerWrapper(
            new RestApiManager(
                details.ApiKey,
                details.ApiSecret,
                TimeSpan.FromSeconds(40)),
            selectedToImport ?? throw new ArgumentNullException(nameof(selectedToImport)));
    }

    public void CloseConnection(object? connection)
    {
        switch (connection)
        {
            case RestApiManager restApiManager:
                restApiManager.Dispose();
                break;
            case RestApiManagerWrapper restApiManagerWrapper:
                restApiManagerWrapper.Dispose();
                break;
            default:
                throw new ArgumentException("Invalid connection type provided.");
        }
    }

    public string SelectToImport(object? connectionDetails)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }
        using var restApiManager = new RestApiManager(details.ApiKey, details.ApiSecret, TimeSpan.FromSeconds(40));
        var groups = restApiManager
            .GetGroupsAsync(CancellationToken.None)
            .ToBlockingEnumerable();
        if (!groups.Any())
        {
            throw new Exception("No groups found in Fivetran account.");
        }

        var consoleOutputBuilder = new StringBuilder();
        consoleOutputBuilder.AppendLine("Available groups in Fivetran account:");
        var elementIndex = 1;
        foreach (var group in groups)
        {
            consoleOutputBuilder.AppendLine($"{elementIndex++}. {group.Name} (ID: {group.Id})");
        }
        consoleOutputBuilder.Append("Please select a group to import from (by number): ");
        Console.Write(consoleOutputBuilder.ToString());
        var input = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(input)
            || !int.TryParse(input, out var selectedIndex)
            || selectedIndex < 1
            || selectedIndex > groups.Count())
        {
            throw new ArgumentException("Invalid group selection.");
        }

        var selectedGroup = groups.ElementAt(selectedIndex - 1);
        return selectedGroup.Id;
    }

    public void RunImport(object? connection)
    {
        if (connection is not RestApiManagerWrapper restApiManagerWrapper)
        {
            throw new ArgumentException("Invalid connection type provided.");
        }

        var restApiManager = restApiManagerWrapper.RestApiManager;
        var groupId = restApiManagerWrapper.GroupId;

        var connectors = restApiManager
            .GetConnectorsAsync(groupId, CancellationToken.None)
            .ToBlockingEnumerable();
        if (!connectors.Any())
        {
            throw new Exception("No connectors found in the selected group.");
        }

        var allMappingsBuffer = "Lineage mappings:\n";
        Parallel.ForEach(connectors, connector =>
        {
            var connectorSchemas = restApiManager
                .GetConnectorSchemasAsync(connector.Id, CancellationToken.None)
                .Result;

            foreach (var schema in connectorSchemas?.Schemas ?? [])
            {
                foreach (var table in schema.Value?.Tables ?? [])
                {
                    allMappingsBuffer += $"  {connector.Id}: {schema.Key}.{table.Key} -> {schema.Value?.NameInDestination}.{table.Value.NameInDestination}\n";
                }
            }
        });

        Console.WriteLine(allMappingsBuffer);
    }
}