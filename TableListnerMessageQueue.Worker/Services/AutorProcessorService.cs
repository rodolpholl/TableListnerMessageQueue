using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using TableListnerMessageQueue.Worker.Models;

namespace AutorProcessor;

public class AutorProcessorService : BackgroundService
{
    private readonly ILogger<AutorProcessorService> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _oracleConnectionString;
    private readonly string _rabbitMQHostname;
    private readonly string _rabbitMQQueueName;
    private readonly string _oracleAQQueueName;

    private IConnection? _rabbitMQConnection;
    private IModel? _rabbitMQChannel;

    private readonly Random _random = new();
    private readonly List<string> _categorias = new() { "ROMANCE", "AUTOAJUDA", "CIENTÍFICO" };

    public AutorProcessorService(ILogger<AutorProcessorService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _oracleConnectionString = _configuration.GetConnectionString("OracleDb") ?? throw new InvalidOperationException("Oracle connection string not found");
        _rabbitMQHostname = _configuration["RabbitMQ:Hostname"] ?? "localhost";
        _rabbitMQQueueName = _configuration["RabbitMQ:QueueName"] ?? "livro_criacao_queue";
        _oracleAQQueueName = _configuration["OracleAQ:QueueName"] ?? "AUTOR_NOVOS_JSON_QUEUE";
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("AutorProcessor iniciando...");

        // Configura RabbitMQ
        var factory = new ConnectionFactory() { HostName = _rabbitMQHostname };
        try
        {
            _rabbitMQConnection = await factory.CreateConnectionAsync();
            _rabbitMQChannel = _rabbitMQConnection.CreateModel();
            _rabbitMQChannel.QueueDeclare(queue: _rabbitMQQueueName,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
            _logger.LogInformation("Conectado ao RabbitMQ. Fila '{QueueName}' declarada.", _rabbitMQQueueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao conectar com RabbitMQ");
            throw;
        }

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AutorProcessor executando. Aguardando mensagens do Oracle AQ...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var autorMensagem = await DequeueFromOracleAQ(stoppingToken);

                if (autorMensagem != null)
                {
                    _logger.LogInformation("Mensagem AQ recebida para Autor ID: {AutorId}, Nome: {AutorNome}",
                        autorMensagem.Id, autorMensagem.Nome);

                    var livroMensagem = GerarDadosLivro(autorMensagem);
                    await PublishToRabbitMQ(livroMensagem);

                    _logger.LogInformation("Livro para Autor ID {AutorId} publicado no RabbitMQ. Título: {Titulo}",
                        autorMensagem.Id, livroMensagem.Titulo);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("AutorProcessor cancelado");
                break;
            }
            catch (OracleException oex) when (oex.Number == 25228 || oex.Number == 25254)
            {
                _logger.LogDebug("Nenhuma mensagem no Oracle AQ (timeout)");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro no loop principal do AutorProcessor");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("AutorProcessor parando...");
        _rabbitMQChannel?.Close();
        _rabbitMQConnection?.Close();
        _rabbitMQChannel?.Dispose();
        _rabbitMQConnection?.Dispose();
        await base.StopAsync(cancellationToken);
    }

    private async Task<AutorMensagem?> DequeueFromOracleAQ(CancellationToken stoppingToken)
    {
        using var connection = new OracleConnection(_oracleConnectionString);
        await connection.OpenAsync(stoppingToken);

        using var queue = new OracleAQQueue(_oracleAQQueueName, connection);

        var dequeueOptions = new OracleAQDequeueOptions
        {
            DequeueMode = OracleAQDequeueMode.Remove,
            Visibility = OracleAQMessageVisibility.OnCommit,
            Wait = 1
        };

        try
        {
            OracleAQMessage message = await queue.DequeueAsync(dequeueOptions, stoppingToken);

            if (message?.Payload?.Content != null)
            {
                string jsonPayload = message.Payload.Content.Text;

                if (!string.IsNullOrWhiteSpace(jsonPayload))
                {
                    return JsonSerializer.Deserialize<AutorMensagem>(jsonPayload);
                }
            }
        }
        catch (OracleException oex) when (oex.Number == 25228 || oex.Number == 25254)
        {
            return null;
        }

        return null;
    }

    private LivroMensagem GerarDadosLivro(AutorMensagem autor)
    {
        var titulo = $"Livro cadastrado para o autor {autor.Nome}";
        var numPaginas = _random.Next(50, 201);
        var categoria = _categorias[_random.Next(_categorias.Count)];

        return new LivroMensagem
        {
            AutorId = autor.Id,
            Titulo = titulo,
            NumPaginas = numPaginas,
            Categoria = categoria
        };
    }

    private async Task PublishToRabbitMQ(LivroMensagem livro)
    {
        var jsonLivro = JsonSerializer.Serialize(livro);
        var body = Encoding.UTF8.GetBytes(jsonLivro);

        _rabbitMQChannel?.BasicPublish(exchange: "",
                                     routingKey: _rabbitMQQueueName,
                                     basicProperties: null,
                                     body: body);
        await Task.CompletedTask;
    }
}