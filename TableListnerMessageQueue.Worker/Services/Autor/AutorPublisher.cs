using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using TableListnerMessageQueue.Worker.Models;

namespace TableListnerMessageQueue.Worker.Services.Autor
{
    public class AutorPublisher: PublisherBase<LivroMensagem>
    {

        private const string RABBITMQ_QUEUE_NAME = "livro_criacao_queue";
      
        private readonly Random _random = new();
        private readonly List<string> _categorias = new() { "ROMANCE", "AUTOAJUDA", "CIENTÍFICO" };

        public AutorPublisher(ILogger<PublisherBase<LivroMensagem>> logger, IConfiguration configuration) : 
            base(logger, configuration) {}

        public LivroMensagem GerarDadosLivro(AutorMensagem autor)
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

        public Task PublishToRabbitMQ(LivroMensagem livro)
        => base.PublishToRabbitMQ(RABBITMQ_QUEUE_NAME, livro);
        

        
    }
}
