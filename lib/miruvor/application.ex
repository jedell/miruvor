defmodule Miruvor.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Logger

  def convert!("true") do
    true
  end

  def convert!(false) do
    false
  end

  @impl true
  def start(_type, _args) do
    topologies = Application.get_env(:libcluster, :topologies, [])
    leader = convert!(Application.get_env(:miruvor, :leader, false))

    Logger.info("Starting Miruvor")
    Logger.info("Node: #{Node.self()}")
    Logger.info("Leader: #{leader}")

    # create list of init nodes including self and sort
    nodes = Application.get_env(:miruvor, :hosts, [])

    # sort initial nodes
    nodes = Enum.sort(nodes)

    Logger.notice("Nodes: #{inspect(nodes)}")

    # create shard table
    shards = Miruvor.Shard.new(nodes)

    Logger.notice("Shards: #{inspect(shards)}")

    children = [
      # Start libcluster
      {Cluster.Supervisor, [topologies, [name: MyApp.ClusterSupervisor]]},
      # Start the Telemetry supervisor
      MiruvorWeb.Telemetry,
      # Start the PubSub system
      {Phoenix.PubSub, name: Miruvor.PubSub},
      # Start the Endpoint (http/https)
      MiruvorWeb.Endpoint,
      # Start a worker by calling: Miruvor.Worker.start_link(arg)
      # {Miruvor.Worker, arg}
      # Start RockDB Storage
      {Miruvor.Storage, [{:node, Node.self(), :shards, shards}]},
      # # Start Raft
      {Miruvor.Raft, [{:node, Node.self(), :is_leader, leader}]}
      # {Miruvor.Servertest, [{:node, Node.self(), :is_leader, leader}]}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Miruvor.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    MiruvorWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
