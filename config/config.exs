# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

# Configures the endpoint
config :miruvor, MiruvorWeb.Endpoint,
  url: [host: "localhost"],
  render_errors: [view: MiruvorWeb.ErrorView, accepts: ~w(json), layout: false],
  pubsub_server: Miruvor.PubSub,
  live_view: [signing_salt: "lFZnLcxn"]

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :miruvor, :hosts, [:"a@127.0.0.1", :"b@127.0.0.1", :"c@127.0.0.1", :"d@127.0.0.1"]

config :libcluster,
  topologies: [
    epmd: [
      # The selected clustering strategy. Required.
      strategy: Cluster.Strategy.Epmd,
      # Configuration for the provided strategy. Optional.
      config: [hosts: [:"a@127.0.0.1", :"b@127.0.0.1", :"c@127.0.0.1", :"d@127.0.0.1"]],
      # The function to use for connecting nodes. The node
      # name will be appended to the argument list. Optional
      connect: {:net_kernel, :connect_node, []},
      # The function to use for disconnecting nodes. The node
      # name will be appended to the argument list. Optional
      disconnect: {:erlang, :disconnect_node, []},
      # The function to use for listing nodes.
      # This function must return a list of node names. Optional
      list_nodes: {:erlang, :nodes, [:connected]},
    ]
  ]

config :miruvor, leader: System.get_env("LEADER") || false

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
