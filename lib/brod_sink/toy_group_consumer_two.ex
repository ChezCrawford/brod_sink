defmodule BrodSink.ToyGroupTwoConsumer do
  @behaviour :brod_group_subscriber_v2

  require Logger

  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end

  ## Client API

  def start_link(topic) do
    client_id = :kafka_client
    consumer_group_name = "brod_toy_consumer_from_start"

    {:ok, _pid} =
      :brod_group_subscriber_v2.start_link(
        %{
          client: client_id,
          group_id: consumer_group_name,
          topics: [topic],
          # GroupConfig
          group_config: [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5],
          # ConsumerConfig
          consumer_config: [begin_offset: :earliest, offset_reset_policy: :reset_to_earliest],
          # CbMod
          cb_module: __MODULE__,
          # CbInitArg
          init_data: [],
          message_type: :message
        }
    )
  end

  ## Per-Partition Callbacks

  def init(init_info = %{partition: partition, topic: topic, commit_fun: commit_fun}, cb_config) do
    Logger.metadata(partition_id: partition)
    Logger.info("Starting group subscriber v2, partition #{partition}, topic: #{topic}")
    # Logger.info("Init Info: #{inspect(init_info)}, Cb Config: #{inspect(cb_config)}")

    {:ok, %{partition: partition, topic: topic, commit_fun: commit_fun}}
  end

  def handle_message(kafka_message(offset: offset, key: key, ts: _ts) = message, state) do
    message_record = kafka_message(message)
    Logger.metadata(offset: offset)
    Logger.info("received message #{inspect(message_record)}", [pid: self()])
    Logger.info("Current state: #{inspect(state)} - pid #{inspect(self())}")

    %{partition: _partition, topic: _topic, commit_fun: commit_fun} = state

    case key do
      "0" ->
        Logger.info("Committing with fun")
        commit_fun.(offset)
        {:ok, state}
      "1" ->
        {:ok, :commit, state}
      "2" ->
        # Commits
        {:ok, :commit, state}
      "3" ->
        # Crashes app
        {:error}
      "4" ->
        {:error, state}
      "5" ->
        # Tell the consumer to do nothing, cb might be used.
        {:ok, state}
      _ ->
        {:ok, :commit, state}
    end
  end
end
