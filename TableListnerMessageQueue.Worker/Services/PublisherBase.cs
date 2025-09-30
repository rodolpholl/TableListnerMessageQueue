using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TableListnerMessageQueue.Worker.Models;
using TableListnerMessageQueue.Worker.Services.Autor;
using TableListnerMessageQueue.Worker.Services.Cliente;

namespace TableListnerMessageQueue.Worker.Services
{
    public interface IPubllisherBase
    {

    }
    public abstract class PublisherBase<T> where T : class, IMessageMQ
    {

        private readonly ILogger<PublisherBase<T>> _logger;
        private readonly IConfiguration _configuration;

        private readonly string _rabbitMQUsername;
        private readonly string _rabbitMQPassword;
        private readonly string _rabbitMQHostname;

        private  IConnection? _rabbitMQConnection;
        private  IChannel? _rabbitMQChannel;

        protected PublisherBase(ILogger<PublisherBase<T>> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            _rabbitMQHostname = _configuration["RabbitMQ:Hostname"] ?? "localhost";
            _rabbitMQUsername = _configuration["RabbitMQ:Username"] ?? "admin";
            _rabbitMQPassword = _configuration["RabbitMQ:Password"] ?? "admin123";

            // Configura RabbitMQ
            var factory = new ConnectionFactory() { HostName = _rabbitMQHostname, UserName = _rabbitMQUsername, Password = _rabbitMQPassword };
            try
            {
                _rabbitMQConnection = Task.Run(() => factory.CreateConnectionAsync()).GetAwaiter().GetResult();
                _rabbitMQChannel = Task.Run(() =>_rabbitMQConnection.CreateChannelAsync()).GetAwaiter().GetResult();

                if (_rabbitMQChannel == null)
                    throw new InvalidOperationException("Não foi possível criar o canal do RabbitMQ");
                

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao conectar com RabbitMQ");
                throw;
            }

           
        }


        protected virtual async Task PublishToRabbitMQ(string queue, T mensagem)
        {

            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };

            await _rabbitMQChannel.QueueDeclareAsync(
                queue: queue,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            _logger.LogInformation("Conectado ao RabbitMQ. Fila '{QueueName}' declarada.", queue);


            var jsonLivro = JsonSerializer.Serialize(mensagem, options);
            var body = Encoding.UTF8.GetBytes(jsonLivro);

            // Corrigido: especificando explicitamente o tipo de argumento como 'IBasicProperties'
            await _rabbitMQChannel.BasicPublishAsync(
                exchange: "",
                routingKey: queue,
                mandatory: false,
                basicProperties: new BasicProperties(),
                body: body
            );

        }
    }
}
