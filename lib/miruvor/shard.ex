defmodule Miruvor.Shard do

  @moduledoc """
  The shard manager for Miruvor.
  """

  alias __MODULE__
  alias Miruvor.Storage
  alias Miruvor.RocksDB
  require Logger

  defstruct(table: nil, size: 0)


  @doc """
  Create a lookup table for shards. This is a map from shard_id to node.
  Take a list of nodes and returns a map of shard_ids based on their index in the list.
  """
  def new(nodes) do
    table = Enum.with_index(nodes)
    |> Enum.map(fn {node, index} -> {index, node} end)
    |> Map.new()

    %Shard{
      table: table,
      size: Enum.count(nodes)
    }
  end

  @doc """
  Get the node for a given shard_id from the lookup table.
  """
  def get_node(shard, shard_id) do
    shard.table[shard_id]
  end

  @doc """
  Get the shard_id for a given key.
  """
  def get_shard_id(shard, key) do
    shard_id = :erlang.phash2(key, shard.size)
    |> rem(shard.size)

    shard_id
  end

end
