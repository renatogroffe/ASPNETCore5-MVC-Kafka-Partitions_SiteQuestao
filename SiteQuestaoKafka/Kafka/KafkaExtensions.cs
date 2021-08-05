using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;

namespace SiteQuestaoKafka.Kafka
{
    public static class KafkaExtensions
    {
        private static int _QtdPartitions = 1;

        public static int QtdPartitions
        {
            get => _QtdPartitions;
        }

        public static void CheckNumPartitions(
            IConfiguration configuration)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = configuration["ApacheKafka:Host"],
            }).Build();
            
            var infoTopic = adminClient.GetMetadata(configuration["ApacheKafka:Topic"],
                TimeSpan.FromSeconds(25)).Topics.FirstOrDefault();
            if (infoTopic is not null)
                _QtdPartitions = infoTopic.Partitions.Count;
        }
    }
}