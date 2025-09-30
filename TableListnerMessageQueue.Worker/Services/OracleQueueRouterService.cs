using Oracle.ManagedDataAccess.Client;
using RabbitMQ.Client;
using System.Data;
using System.Text.Json;
using TableListnerMessageQueue.Worker.Models;
using TableListnerMessageQueue.Worker.Services.Autor;
using TableListnerMessageQueue.Worker.Services.Cliente;

namespace TableListnerMessageQueue.Worker.Services;

public class OracleQueueRouterService : BackgroundService
{
    private readonly ILogger<OracleQueueRouterService> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _oracleConnectionString;
    
    private readonly string _oracleAQQueueName;
    

    //Publish services
    private readonly ClientePublisher _clientePublisher;
    private readonly AutorPublisher _autorPublisher;


    private IConnection? _rabbitMQConnection;
    private IChannel? _rabbitMQChannel;
    private  Dictionary<int, IMessageStrategy> _messageStrategies = default!;

    

    public OracleQueueRouterService(ILogger<OracleQueueRouterService> logger, IConfiguration configuration, AutorPublisher autorPublisher, ClientePublisher clientePublisher)
    {
        _logger = logger;
        _configuration = configuration;
        _oracleConnectionString = _configuration.GetConnectionString("OracleDb") ?? throw new InvalidOperationException("Oracle connection string not found");
        
        _oracleAQQueueName = _configuration["OracleAQ:QueueName"] ?? "AUTOR_NOVOS_JSON_QUEUE";
        
        _autorPublisher = autorPublisher;
        _clientePublisher = clientePublisher;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("AutorProcessor iniciando...");
        

        _messageStrategies = new Dictionary<int, IMessageStrategy>
            {
                { 1, new AutorMessageStrategy(_autorPublisher) },
                { 2, new ClienteMessageStrategy(_clientePublisher) }
            };

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AutorProcessor executando. Aguardando mensagens do Oracle AQ...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DequeueFromOracleAQ(stoppingToken);
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

    private async Task DequeueFromOracleAQ(CancellationToken stoppingToken)
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
            Size = 32767
        });

        try
        {
            _logger.LogDebug("Iniciando dequeue da fila {QueueName}", _oracleAQQueueName);
            
            await cmd.ExecuteNonQueryAsync(stoppingToken);
            
            var jsonPayload = cmd.Parameters["message_text"].Value?.ToString();

            if (!string.IsNullOrWhiteSpace(jsonPayload))
            {
                _logger.LogDebug("JSON recebido: {Json}", jsonPayload);

                var dbQueueMensagem = JsonSerializer.Deserialize<DbQueueModel>(jsonPayload);
                if (dbQueueMensagem != null)
                {
                    _logger.LogInformation("Mensagem deserializada com sucesso");
                    
                    if (_messageStrategies.TryGetValue((int)dbQueueMensagem.Table, out var strategy))
                    {
                        await strategy.ProcessMessage(dbQueueMensagem.Data);
                    }
                    else
                    {
                        _logger.LogWarning("Tipo de mensagem não suportado: {Table}", dbQueueMensagem.Table);
                    }
                }
            }
            else
            {
                _logger.LogDebug("Nenhuma mensagem disponível na fila");
            }

        }
        catch (OracleException oex) when (oex.Number == 25228 || oex.Number == 25254)
        {
            _logger.LogDebug("Timeout ao aguardar mensagem");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao processar mensagem da fila Oracle AQ");
            throw;
        }
    }

}