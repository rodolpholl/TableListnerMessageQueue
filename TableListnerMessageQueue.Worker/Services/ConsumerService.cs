using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using TableListnerMessageQueue.Worker.Models;

namespace TableListnerMessageQueue.Worker.Services;

public class ConsumerService : BackgroundService
{
    private readonly ILogger<ConsumerService> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _oracleConnectionString;
    private readonly string _rabbitMQHostname;
    private readonly string _rabbitMQQueueName;
    private readonly string _rabbitMQUsername;
    private readonly string _rabbitMQPassword;

    private IConnection? _rabbitMQConnection;
    private IChannel? _rabbitMQChannel;

    public ConsumerService(ILogger<ConsumerService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _oracleConnectionString = _configuration.GetConnectionString("OracleDb") ?? throw new InvalidOperationException("Oracle connection string not found");
        _rabbitMQHostname = _configuration["RabbitMQ:Hostname"] ?? "localhost";
        _rabbitMQQueueName = _configuration["RabbitMQ:QueueName"] ?? "livro_criacao_queue";
        _rabbitMQUsername = _configuration["RabbitMQ:Username"] ?? "admin";
        _rabbitMQPassword = _configuration["RabbitMQ:Password"] ?? "admin123";
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Consumer Service iniciando...");

        var factory = new ConnectionFactory
        {
            HostName = _rabbitMQHostname,
            UserName = _rabbitMQUsername,
            Password = _rabbitMQPassword
        };

        try
        {
            _rabbitMQConnection = await factory.CreateConnectionAsync(cancellationToken);
            _rabbitMQChannel = await _rabbitMQConnection.CreateChannelAsync();

            if (_rabbitMQChannel == null)
            {
                throw new InvalidOperationException("Não foi possível criar o canal do RabbitMQ");
            }

            await _rabbitMQChannel.QueueDeclareAsync(
                queue: _rabbitMQQueueName,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            await _rabbitMQChannel.BasicQosAsync(0, 1, false);

            var consumer = new AsyncEventingBasicConsumer(_rabbitMQChannel);
            consumer.ReceivedAsync += ProcessMessageAsync;

            await _rabbitMQChannel.BasicConsumeAsync(
                queue: _rabbitMQQueueName,
                autoAck: false,
                consumer: consumer);

            _logger.LogInformation("Conectado ao RabbitMQ. Aguardando mensagens na fila '{QueueName}'", _rabbitMQQueueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao conectar com RabbitMQ");
            throw;
        }

        await base.StartAsync(cancellationToken);
    }

    private async Task ProcessMessageAsync(object? model, BasicDeliverEventArgs ea)
    {
        string? jsonMessage = null;
        try
        {
            var body = ea.Body.ToArray();
            jsonMessage = Encoding.UTF8.GetString(body);

            _logger.LogInformation("Mensagem recebida: {Message}", jsonMessage);

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            var livroMensagem = JsonSerializer.Deserialize<LivroMensagem>(jsonMessage, options);

            if (livroMensagem != null)
            {
                await InserirLivro(livroMensagem);
                await _rabbitMQChannel!.BasicAckAsync(ea.DeliveryTag, false);
                _logger.LogInformation("Livro processado com sucesso. Título: {Titulo}", livroMensagem.Titulo);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Erro ao deserializar mensagem: {Message}", jsonMessage);
            if (_rabbitMQChannel != null)
                await _rabbitMQChannel.BasicNackAsync(ea.DeliveryTag, false, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao processar mensagem: {Message}", jsonMessage);
            if (_rabbitMQChannel != null)
                await _rabbitMQChannel.BasicNackAsync(ea.DeliveryTag, false, true);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer Service cancelado");
        }
    }

    private async Task InserirLivro(LivroMensagem livro)
    {
        using var connection = new OracleConnection(_oracleConnectionString);
        await connection.OpenAsync();

        using var transaction = connection.BeginTransaction();
        try
        {
            using var cmd = new OracleCommand
            {
                Connection = connection,
                Transaction = transaction,
                CommandText = @"
                    INSERT INTO C##_TESTEMSG.LIVRO 
                    (AUTOR_ID, TITULO, NUM_PAGINAS, CATEGORIA) 
                    VALUES 
                    (:autorId, :titulo, :numPaginas, :categoria)"
            };

            cmd.Parameters.Add("autorId", OracleDbType.Int64).Value = livro.AutorId;
            cmd.Parameters.Add("titulo", OracleDbType.Varchar2).Value = livro.Titulo;
            cmd.Parameters.Add("numPaginas", OracleDbType.Int32).Value = livro.NumPaginas;
            cmd.Parameters.Add("categoria", OracleDbType.Varchar2).Value = livro.Categoria;

            await cmd.ExecuteNonQueryAsync();
            await transaction.CommitAsync();

            _logger.LogInformation("Livro inserido com sucesso. Título: {Titulo}", livro.Titulo);
        }
        catch (Exception)
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Consumer Service parando...");

        try
        {
            if (_rabbitMQChannel != null)
            {
                await _rabbitMQChannel.CloseAsync();
            }
            if (_rabbitMQConnection != null)
            {
                await _rabbitMQConnection.CloseAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao fechar conexões do RabbitMQ");
        }
        finally
        {
            _rabbitMQChannel?.Dispose();
            _rabbitMQConnection?.Dispose();
        }

        await base.StopAsync(cancellationToken);
    }
}
