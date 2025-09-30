using TableListnerMessageQueue.Worker.Services;
using TableListnerMessageQueue.Worker.Services.Autor;
using TableListnerMessageQueue.Worker.Services.Cliente;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<OracleQueueRouterService>();
builder.Services.AddHostedService<ConsumerService>();

// Adding Publioshers as Singleton services
builder.Services.AddSingleton<AutorPublisher>();
builder.Services.AddSingleton<ClientePublisher>();

// Adding Consumers as Singleton services
builder.Services.AddSingleton<AutorConsumer>();
builder.Services.AddSingleton<ClienteConsumer>();


var host = builder.Build();
host.Run();
