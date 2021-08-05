using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;

namespace SiteQuestaoKafka.Kafka
{
    public class VotacaoProducer
    {
        private static readonly PartitionSelector _PARTITION_SELECTOR = new PartitionSelector();
        private readonly int _partition;
        private readonly ILogger<VotacaoProducer> _logger;
        private readonly IConfiguration _configuration;
        private readonly JsonSerializerOptions _serializerOptions;

        public VotacaoProducer(
            ILogger<VotacaoProducer> logger,
            IConfiguration configuration)
        {
            int nextPartition;
            lock (_PARTITION_SELECTOR)
            {
                nextPartition = _PARTITION_SELECTOR.Next();
            }
            _partition = nextPartition;

            _logger = logger;
            _configuration = configuration;
            _serializerOptions = new JsonSerializerOptions()
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        private async Task SendEventDataAsync<T>(IProducer<Null, string> producer, T eventData)
        {
            string topic = _configuration["ApacheKafka:Topic"];

            string data = JsonSerializer.Serialize(eventData, _serializerOptions);
            _logger.LogInformation($"Evento: {data}");

            var result = await producer.ProduceAsync(
                new TopicPartition(topic, new Partition(_partition)),
                new Message<Null, string>
                { Value = data });

            _logger.LogInformation(
                $"Apache Kafka - Envio para o tópico {topic} concluído | " +
                $"{data} | Status: { result.Status.ToString()}");
        }

        public async Task Send(string tecnologia)
        {
            var configKafka = new ProducerConfig
            {
                BootstrapServers = _configuration["ApacheKafka:Host"]
            };

            using (var producer = new ProducerBuilder<Null, string>(configKafka).Build())
            {
                var idVoto = Guid.NewGuid().ToString();
                var horario = $"{DateTime.UtcNow.AddHours(-3):yyyy-MM-dd HH:mm:ss}";

                await SendEventDataAsync<Voto>(producer,
                    new ()
                    {
                        IdVoto = idVoto,
                        Instancia = Environment.MachineName,
                        Horario = horario,
                        Tecnologia = tecnologia
                    });
            }

            _logger.LogInformation("Concluido o envio dos eventos!");
        }
    }
}