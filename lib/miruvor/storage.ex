defmodule Miruvor.Storage do
  use GenServer
  require Logger

  alias Miruvor.RocksDB

  @moduledoc """
  The storage for Miruvor.
  """

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(opt) do
    Logger.info("Starting RocksDB Storage")
    [{:node, node}] = opt
    db = RocksDB.create_table(node)
    GenServer.start_link(__MODULE__, [db_handle: db], name: __MODULE__)
  end

  @spec init(any) :: {:ok, any}
  def init(args) do
    [{:db_handle, db_handle}] = args
    {:ok, db_handle}
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

  def handle_call({:get, key}, _from, db_handle) do
    Logger.warn("handle_call")
    case RocksDB.get(db_handle, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, db_handle}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, db_handle}

      {:error, reason} ->
        {:reply, {:error, reason}, db_handle}
    end
  end

  def handle_call({:put, key, value}, _from, db_handle) do
    case RocksDB.put(db_handle, key, value) do
      {:ok, value} ->
        {:reply, {:ok, value}, db_handle}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, db_handle}

      {:error, reason} ->
        {:reply, {:error, reason}, db_handle}
    end
  end

  def handle_call({:delete, key}, _from, db_handle) do
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
