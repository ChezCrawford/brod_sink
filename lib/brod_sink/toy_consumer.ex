defmodule BrodSink.ToyConsumer do
  @behaviour :brod_topic_subscriber

  import Record, only: [defrecord: 2, extract: 2]

  require Logger
  require Record

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def handle_message(
    partition_id,
    message,
    state
    ) do
    message_record = kafka_message(message)
    Logger.info("Partition #{partition_id} received message #{inspect(message_record)}", [pid: self()])
    Logger.info("Current state: #{inspect(state)} - pid #{inspect(self())}")
    {:ok, state}
  end

  def start_link(topic) do
    client_id = String.to_atom("kafka_client_#{topic}")
    client_options = []
    hosts = [{"kafka.pagerduty.net", 9092}]
    :ok = :brod.start_client(hosts, client_id, client_options)

    {:ok, _pid} =
      :brod_topic_subscriber.start_link(
        client_id,
        topic,
        :all,
        # ConsumerConfig
        [begin_offset: :latest],
        # CbMod
        __MODULE__,
        # CbInitArg
        __MODULE__
      )
  end

  def init(topic, handler) do
    Logger.info("Initializing topic #{topic} with handler #{handler}")
    {:ok, [], %{}}
  end
end
