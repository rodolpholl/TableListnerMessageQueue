using RabbitMQ.Client;
using System.Text.Json;
using TableListnerMessageQueue.Worker.Services;
using TableListnerMessageQueue.Worker.Services.Autor;
using TableListnerMessageQueue.Worker.Services.Cliente;

namespace TableListnerMessageQueue.Worker.Models;


public interface IMessageStrategy
{
    Task ProcessMessage(string data);
    
}

public interface IMessageMQ
{
   
}

public class AutorMessageStrategy(AutorPublisher autorPublisher) : IMessageStrategy
{
    public async Task ProcessMessage(string data)
    {
        var autorMensagem = JsonSerializer.Deserialize<AutorMensagem>(data);
        if (autorMensagem == null)
        {
            throw new InvalidOperationException("Falha ao deserializar mensagem do autor");
        }

        var livroMensagem = autorPublisher.GerarDadosLivro(autorMensagem);
        await autorPublisher.PublishToRabbitMQ(livroMensagem);
    }
}

public class ClienteMessageStrategy(ClientePublisher clientePublisher) : IMessageStrategy
{
 
    public async Task ProcessMessage(string data)
    {
        var clienteMensagem = JsonSerializer.Deserialize<ClienteAutorFavoritoMessage>(data);
        if (clienteMensagem == null)
        {
            throw new InvalidOperationException("Falha ao deserializar mensagem do cliente");
        }

        var autorFavorito = clientePublisher.GerarAutorFavorito(clienteMensagem);
        await clientePublisher.PublishToRabbitMQ(autorFavorito);
    }
}