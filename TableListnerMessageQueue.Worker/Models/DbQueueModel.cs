using System.Text.Json.Serialization;

namespace TableListnerMessageQueue.Worker.Models;

public enum TableMessagesEnum
{
    Autor = 1,
    Cliente = 2
}

public record DbQueueModel
{
    [JsonPropertyName("table")]
    public TableMessagesEnum Table { get; set; } = default!;
    [JsonPropertyName("data")]
    public string Data { get; set; } = default!;
}

