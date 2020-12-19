using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.ServiceBus;
using WorkerAcoes.Data;
using WorkerAcoes.Models;
using WorkerAcoes.Validators;

namespace WorkerAcoes
{
    public class Worker : IHostedService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly AcoesRepository _repository;
        private static ISubscriptionClient _subscriptionClient;

        public Worker(ILogger<Worker> logger, IConfiguration configuration,
            AcoesRepository repository)
        {
            _logger = logger;
            _configuration = configuration;
            _repository = repository;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
           _logger.LogInformation("Testando o consumo de mensagens com Azure Service Bus");

            string nomeTopic = _configuration["AzureServiceBus:Topic"];
            string subscription = _configuration["AzureServiceBus:Subscription"];
            _subscriptionClient = new SubscriptionClient(
                _configuration["AzureServiceBus:ConnectionString"],
                nomeTopic, subscription);

            _logger.LogInformation($"Topic = {nomeTopic}");
            _logger.LogInformation($"Subscription = {subscription}");

            _logger.LogInformation("Aguardando mensagens...");
            RegisterOnMessageHandlerAndReceiveMessages();

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogWarning("Encerrando o processamento de mensagens!");                
            return _subscriptionClient.CloseAsync();
        }

        private void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(
                ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            _subscriptionClient.RegisterMessageHandler(
                ProcessMessagesAsync, messageHandlerOptions);
        }

        private async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            string dados = Encoding.UTF8.GetString(message.Body);
            _logger.LogInformation($"Mensagem recebida: {dados}");

            Acao acao;            
            try
            {
                acao = JsonSerializer.Deserialize<Acao>(dados,
                    new JsonSerializerOptions()
                    {
                        PropertyNameCaseInsensitive = true
                    });
            }
            catch
            {
                acao = null;
            }

            if (acao is not null &&
                new AcaoValidator().Validate(acao).IsValid)
            {
                _repository.Save(acao);
                _logger.LogInformation("Ação registrada com sucesso!");
            }
            else
            {
                _logger.LogError("Dados inválidos para a Ação");
            }

            await _subscriptionClient.CompleteAsync(
                message.SystemProperties.LockToken);
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            _logger.LogError($"Message handler - Tratamento - Exception: {exceptionReceivedEventArgs.Exception}.");

            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            _logger.LogError("Exception context - informaçoes para resolução de problemas:");
            _logger.LogError($"- Endpoint: {context.Endpoint}");
            _logger.LogError($"- Entity Path: {context.EntityPath}");
            _logger.LogError($"- Executing Action: {context.Action}");

            return Task.CompletedTask;
        }
    }
}