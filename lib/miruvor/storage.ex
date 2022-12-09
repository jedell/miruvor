defmodule Miruvor.Storage do
  use GenServer
  require Logger

  alias Miruvor.RocksDB
  alias Miruvor.Shard

  @moduledoc """
  The storage for Miruvor.
  """

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(opt) do
    Logger.info("Starting RocksDB Storage")
    [{:node, node, :shards, shards}] = opt
    db = RocksDB.create_table(node)

    GenServer.start_link(__MODULE__, [db_handle: db, shards: shards], name: __MODULE__)
  end

  @spec init(any) :: {:ok, any}
  def init(args) do
    [db_handle: db_handle, shards: shards] = args
    {:ok, {db_handle, shards}}
  end

  def get(key) do
    Logger.warn("get")
    GenServer.call(__MODULE__, {:get, key})
  end

  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  def create_table(table) do
    GenServer.call(__MODULE__, {:create_table, table})
  end

  def create_table(table, path) do
    GenServer.call(__MODULE__, {:create_table, table, path})
  end

  def create_table() do
    GenServer.call(__MODULE__, {:create_table})
  end

  def handle_call({:get, key}, _from, {db_handle, shards}) do
    case RocksDB.get(db_handle, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, db_handle}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, db_handle}

      {:error, reason} ->
        {:reply, {:error, reason}, db_handle}
    end
  end

  def handle_call({:put, key, value}, _from, {db_handle, shards}) do
    shard_id = Shard.get_shard_id(shards, key)
    Logger.warn("Shard ID: #{shard_id})")

    to_node = Shard.get_node(shards, shard_id)
    Logger.warn("To Node: #{to_node})")

    if to_node == Node.self() do
      Logger.debug("Shard is local")

      case RocksDB.put(db_handle, key, value) do
        {:ok, value} ->
          {:reply, {:ok, value}, {db_handle, shards}}

        {:error, :not_found} ->
          {:reply, {:error, :not_found}, {db_handle, shards}}

        {:error, reason} ->
          {:reply, {:error, reason}, {db_handle, shards}}
      end
    else
      Logger.debug("Shard is remote")
      # make call directly to node with valid shard
      if Miruvor.Raft.is_leader?() do
        resp = :rpc.call(to_node, Miruvor.Storage, :put, [key, value])
        Logger.warn("Response: #{inspect(resp)}")
        {:reply, resp, {db_handle, shards}}
      else
        {:reply, {:ok, :not_leader}, {db_handle, shards}}
      end
    end
  end

  def handle_call({:delete, key}, _from, {db_handle, shards}) do
    case RocksDB.delete(db_handle, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, db_handle}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, db_handle}

      {:error, reason} ->
        {:reply, {:error, reason}, db_handle}
    end
  end
end
