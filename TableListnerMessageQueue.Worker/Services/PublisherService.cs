using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using RabbitMQ.Client;
using System.Data;
using System.Text;
using System.Text.Json;
using TableListnerMessageQueue.Worker.Models;

namespace TableListnerMessageQueue.Worker.Services;

public class PublisherService : BackgroundService
{
    private readonly ILogger<PublisherService> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _oracleConnectionString;
    private readonly string _rabbitMQHostname;
    private readonly string _rabbitMQQueueName;
    private readonly string _oracleAQQueueName;
    private readonly string _rabbitMQUsername;
    private readonly string _rabbitMQPassword;

    private IConnection? _rabbitMQConnection;
    private IChannel? _rabbitMQChannel;

    private readonly Random _random = new();
    private readonly List<string> _categorias = new() { "ROMANCE", "AUTOAJUDA", "CIENTÍFICO" };

    public PublisherService(ILogger<PublisherService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _oracleConnectionString = _configuration.GetConnectionString("OracleDb") ?? throw new InvalidOperationException("Oracle connection string not found");
        _rabbitMQHostname = _configuration["RabbitMQ:Hostname"] ?? "localhost";
        _rabbitMQQueueName = _configuration["RabbitMQ:QueueName"] ?? "livro_criacao_queue";
        _oracleAQQueueName = _configuration["OracleAQ:QueueName"] ?? "AUTOR_NOVOS_JSON_QUEUE";
        _rabbitMQUsername = _configuration["RabbitMQ:Username"] ?? "admin";
        _rabbitMQPassword = _configuration["RabbitMQ:Password"] ?? "admin123";
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("AutorProcessor iniciando...");
        // Configura RabbitMQ
        var factory = new ConnectionFactory() { HostName = _rabbitMQHostname, UserName = _rabbitMQUsername, Password = _rabbitMQPassword };
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

    private async Task<AutorMensagem?> DequeueFromOracleAQ(CancellationToken stoppingToken)
    {
        using var connection = new OracleConnection(_oracleConnectionString);
        await connection.OpenAsync(stoppingToken);

        using var cmd = new OracleCommand
        {
            Connection = connection,
            CommandText = $@"
                DECLARE
                    dequeue_options     DBMS_AQ.DEQUEUE_OPTIONS_T;
                    message_properties  DBMS_AQ.MESSAGE_PROPERTIES_T;
                    message_handle     RAW(16);
                    message            SYS.AQ$_JMS_TEXT_MESSAGE;
                BEGIN
                    dequeue_options.consumer_name := NULL;
                    dequeue_options.wait := 1;
                    dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
                    
                    DBMS_AQ.DEQUEUE(
                        queue_name         => '{_oracleAQQueueName}',
                        dequeue_options    => dequeue_options,
                        message_properties => message_properties,
                        payload           => message,
                        msgid             => message_handle
                    );
                    
                    :message_text := message.text_vc;
                    COMMIT;
                END;"
        };

        cmd.Parameters.Add(new OracleParameter("message_text", OracleDbType.Varchar2, ParameterDirection.Output)
        {
            Size = 4000
        });

        try
        {
            _logger.LogDebug("Iniciando dequeue da fila {QueueName}", _oracleAQQueueName);
            
            await cmd.ExecuteNonQueryAsync(stoppingToken);
            
            var jsonPayload = cmd.Parameters["message_text"].Value?.ToString();

            if (!string.IsNullOrWhiteSpace(jsonPayload))
            {
                _logger.LogDebug("JSON recebido: {Json}", jsonPayload);

                var options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                };

                var autorMensagem = JsonSerializer.Deserialize<AutorMensagem>(jsonPayload, options);
                if (autorMensagem != null)
                {
                    _logger.LogInformation("Mensagem deserializada com sucesso");
                    return autorMensagem;
                }
            }
            else
            {
                _logger.LogDebug("Nenhuma mensagem disponível na fila");
            }

            return null;
        }
        catch (OracleException oex) when (oex.Number == 25228 || oex.Number == 25254)
        {
            _logger.LogDebug("Timeout ao aguardar mensagem");
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao processar mensagem da fila Oracle AQ");
            throw;
        }
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

    private  Task PublishToRabbitMQ(LivroMensagem livro)
    {
        if (_rabbitMQChannel == null)
        {
            throw new InvalidOperationException("Canal do RabbitMQ não está disponível");
        }

        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        var jsonLivro = JsonSerializer.Serialize(livro, options);
        var body = Encoding.UTF8.GetBytes(jsonLivro);

        // Corrigido: especificando explicitamente o tipo de argumento como 'IBasicProperties'
        _rabbitMQChannel.BasicPublishAsync(
            exchange: "",
            routingKey: _rabbitMQQueueName,
            mandatory: false,
            basicProperties: new BasicProperties(),
            body: body
        );

        return Task.CompletedTask;
    }
}