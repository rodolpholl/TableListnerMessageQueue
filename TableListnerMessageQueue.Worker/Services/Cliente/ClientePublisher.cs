using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using TableListnerMessageQueue.Worker.Models;
using TableListnerMessageQueue.Worker.Services.Autor;

namespace TableListnerMessageQueue.Worker.Services.Cliente
{
    public class ClientePublisher : PublisherBase<AutorMensagem>
    {

        private const string RABBITMQ_QUEUE_NAME = "autor_favorito_criacao_queue";

        public ClientePublisher(ILogger<PublisherBase<AutorMensagem>> logger, IConfiguration configuration) : 
            base(logger, configuration){}

        public AutorMensagem GerarAutorFavorito(ClienteAutorFavoritoMessage cliente)
        {
            throw new NotImplementedException();
        }

        public  Task PublishToRabbitMQ(AutorMensagem autorFavorito)
        =>  base.PublishToRabbitMQ(RABBITMQ_QUEUE_NAME, autorFavorito);
        
    }
}
