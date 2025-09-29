using TableListnerMessageQueue.Worker;
using TableListnerMessageQueue.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

// Adicione seus serviços diretamente ao builder.Services
builder.Services.AddHostedService<AutorProcessorService>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
