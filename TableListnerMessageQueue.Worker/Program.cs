using TableListnerMessageQueue.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<PublisherService>();
builder.Services.AddHostedService<ConsumerService>();

var host = builder.Build();
host.Run();
