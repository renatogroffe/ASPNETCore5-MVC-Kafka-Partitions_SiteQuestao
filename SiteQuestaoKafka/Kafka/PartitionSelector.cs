namespace SiteQuestaoKafka.Kafka
{
    public class PartitionSelector
    {
        private int _partition;

        public PartitionSelector()
        {
            _partition = KafkaExtensions.QtdPartitions > 1 ? -1 : 0;
        }

        public int Next()
        {
            if (KafkaExtensions.QtdPartitions > 1)
            {
                _partition++;
                if (_partition == KafkaExtensions.QtdPartitions)
                    _partition = 0;
            }

            return _partition;
        }        
    }
}