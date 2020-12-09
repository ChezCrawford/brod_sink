defmodule BrodSink.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  require Logger

  def start(_type, _args) do
    Logger.metadata(starting: true)
    Logger.info("Starting app")

    topic_names = ["brod_testing"]

    children = [
      # worker(BrodSink.ToyConsumer, topic_names)
      # worker(BrodSink.ToyGroupConsumer, topic_names)
      brod_client_child_spec(),
      {BrodSink.ToyGroupTwoConsumer, topic_names}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: BrodSink.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def brod_client_child_spec() do
    client_id = :kafka_client

    child_args = [
      # bootstrap endpoints
      [{"kafka.pagerduty.net", 9092}],
      # client id
      client_id,
      # Client config
      []
    ]

    %{
      id: client_id,
      start: {
        :brod,
        :start_link_client,
        child_args
      }
    }
  end
end
