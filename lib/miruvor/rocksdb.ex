defmodule Miruvor.RocksDB do
  require Logger

  @moduledoc """
  The RocksDB storage for Miruvor.
  """

  @spec create_table :: :rocksdb.db_handle()
  @doc """
  Create RocksDB Table
  """
  def create_table() do
    path = '/tmp/rocksdb'
    options = [{:create_if_missing, true}]
    {:ok, db} = :rocksdb.open(path, options)
    db
  after
    Logger.info("RocksDB Table created")
  end

  @spec create_table(String) :: :rocksdb.db_handle()
  def create_table(table) do
    :ok = File.mkdir_p!("/tmp/rocksdb")
    path = '/tmp/rocksdb/#{table}'
    options = [{:create_if_missing, true}]
    {:ok, db} = :rocksdb.open(path, options)
    db
  after
    Logger.info("RocksDB Table #{table} created")
  end

  @spec create_table(String, String) :: :rocksdb.db_handle()
  def create_table(table, path) do
    path = '/tmp/rocksdb/#{path}/#{table}'
    options = [{:create_if_missing, true}]
    {:ok, db} = :rocksdb.open(path, options)
    db
  after
    Logger.info("RocksDB Table #{table}/#{path} created")
  end

  @doc """
  Get the value associated with the given key.
  """
  def get(db, key) do
    case :rocksdb.get(db, key, []) do
      {:ok, value} ->
        {:ok, value}

      :not_found ->
        Logger.warn("Key #{key} not found")
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  after
    Logger.info("RocksDB get #{key}")
  end

  def get(db, cf, key) do
    case :rocksdb.get(db, cf, key, []) do
      {:ok, value} ->
        {:ok, value}

      :not_found ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  after
    Logger.info("RocksDB get #{key}")
  end

  @doc """
  Puts the value associated with the given key.
  """
  def put(db, key, value) do
    case :rocksdb.put(db, key, value, []) do
      :ok ->
        {:ok, value}

      {:error, reason} ->
        {:error, reason}
    end
  after
    Logger.info("RocksDB put #{key}")
  end

  def put(db, cf, key, value) do
    case :rocksdb.put(db, cf, key, value, []) do
      :ok ->
        {:ok, value}

      {:error, reason} ->
        {:error, reason}
    end
  after
    Logger.info("RocksDB put #{key}")
  end

  @doc """
  Deletes the value associated with the given key.
  """
  def delete(db, key) do
    case :rocksdb.delete(db, key, []) do
      :ok ->
        {:ok, key}

      {:error, reason} ->
        {:error, reason}
    end
  after
    Logger.info("RocksDB delete #{key}")
  end

  def delete(db, cf, key) do
    case :rocksdb.delete(db, cf, key, []) do
      :ok ->
        {:ok, key}

      {:error, reason} ->
        {:error, reason}
    end
  after
    Logger.info("RocksDB delete #{key}")
  end

  @doc """
  Closes the database.
  """
  def close(db) do
    case :rocksdb.close(db) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end
end
