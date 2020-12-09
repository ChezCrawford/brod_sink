defmodule BrodSink.ToyGroupConsumer do
  @behaviour :brod_group_subscriber

  import Record, only: [defrecord: 2, extract: 2]

  require Logger
  require Record

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def start_link(topic) do
    client_id = String.to_atom("kafka_client_#{topic}")
    client_options = []
    hosts = [{"kafka.pagerduty.net", 9092}]
    :ok = :brod.start_client(hosts, client_id, client_options)

    consumer_group_name = "brod_toy_consumer_alt"

    {:ok, _pid} =
      :brod_group_subscriber.start_link(
        client_id,
        consumer_group_name,
        [topic],
        # GroupConfig
        [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5],
        # ConsumerConfig
        [begin_offset: :latest],
        # CbMod
        __MODULE__,
        # CbInitArg
        __MODULE__
      )
  end


  def init(_group_id, _arg) do
    {:ok, []}
  end

  def handle_message(topic, partition, message, state) do
    message_record = kafka_message(message)
    Logger.info("Topic: #{topic} Partition #{partition} received message #{inspect(message_record)}", [pid: self()])
    {:ok, state}
  end
end
