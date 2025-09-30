using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TableListnerMessageQueue.Worker.Models;
using TableListnerMessageQueue.Worker.Services;

namespace TableListnerMessageQuee.Test;

[TestClass]
public sealed class PublisherServiceTest
{
    private Mock<ILogger<PublisherService>> _loggerMock = null!;
    private Mock<IConfiguration> _configurationMock = null!;
    private PublisherService _publisherService = null!;

    [TestInitialize]
    public void Setup()
    {
        // Setup logger mock
        _loggerMock = new Mock<ILogger<PublisherService>>();

        // Setup configuration mock
        _configurationMock = new Mock<IConfiguration>();

        // Setup connection string
        _configurationMock.Setup(x => x["ConnectionStrings:OracleDb"])
            .Returns("User Id=test;Password=test;Data Source=test");

        _configurationMock.Setup(x => x.GetConnectionString(It.IsAny<string>()))
            .Returns("User Id=test;Password=test;Data Source=test");


        // Setup RabbitMQ configuration
        _configurationMock.Setup(x => x["RabbitMQ:Hostname"]).Returns("localhost");
        _configurationMock.Setup(x => x["RabbitMQ:QueueName"]).Returns("test_queue");
        _configurationMock.Setup(x => x["RabbitMQ:Username"]).Returns("test");
        _configurationMock.Setup(x => x["RabbitMQ:Password"]).Returns("test");
        _configurationMock.Setup(x => x["OracleAQ:QueueName"]).Returns("test_aq_queue");

        // Create service instance
        _publisherService = new PublisherService(
            _loggerMock.Object,
            _configurationMock.Object);
    }

    [TestMethod]
    public async Task StartAsync_ShouldInitializeRabbitMQConnection()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        // Act
        await _publisherService.StartAsync(cancellationToken);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("AutorProcessor iniciando")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);

        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Conectado ao RabbitMQ")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [TestMethod]
    public async Task StopAsync_ShouldCloseConnections()
    {
        // Arrange
        var cancellationToken = new CancellationToken();
        await _publisherService.StartAsync(cancellationToken);

        // Act
        await _publisherService.StopAsync(cancellationToken);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("AutorProcessor parando")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [TestMethod]
    public async Task ExecuteAsync_ShouldHandleOracleAQTimeout()
    {
        // Arrange
        var cancellationToken = new CancellationTokenSource(TimeSpan.FromSeconds(1)).Token;

        // Act & Assert
        try
        {
            await _publisherService.StartAsync(CancellationToken.None);
            await Task.Delay(1500, CancellationToken.None); // Give some time for the service to run

            _loggerMock.Verify(
                x => x.Log(
                    LogLevel.Debug,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Nenhuma mensagem no Oracle AQ")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.AtLeastOnce);
        }
        finally
        {
            await _publisherService.StopAsync(CancellationToken.None);
        }
    }

    [TestMethod]
    public void Constructor_ShouldThrowException_WhenConnectionStringIsMissing()
    {
        // Arrange
        var configMock = new Mock<IConfiguration>();
        configMock.Setup(x => x.GetConnectionString("OracleDb")).Returns((string)null!);

        // Act & Assert
        Assert.ThrowsException<InvalidOperationException>(() => 
            new PublisherService(
                _loggerMock.Object,
                configMock.Object));
    }

    [TestMethod]
    public async Task PublishToRabbitMQ_ShouldThrowException_WhenChannelIsNull()
    {
        // Arrange
        var livroMensagem = new LivroMensagem
        {
            AutorId = 1,
            Titulo = "Test Book",
            NumPaginas = 100,
            Categoria = "TEST"
        };

        // Act & Assert
        var methodInfo = typeof(PublisherService).GetMethod("PublishToRabbitMQ", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            async () => await (Task)methodInfo!.Invoke(_publisherService, new object[] { livroMensagem })!);
    }
}